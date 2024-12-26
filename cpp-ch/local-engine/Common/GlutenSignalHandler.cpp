/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <cstring>
#include <vector>
#include <thread>
#include <Core/Settings.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileDescriptorDiscardOnFailure.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <base/getThreadId.h>
#include <base/phdr_cache.h>
#include <base/sleep.h>
#include <Poco/Exception.h>
#include <Poco/Runnable.h>
#include <Poco/Thread.h>
#include <Common/CurrentThread.h>
#include <Common/GlutenSignalHandler.h>
#include <Common/MemoryTracker.h>
#include <Common/PipeFDs.h>
#include <Common/ThreadStatus.h>
#include <Common/config_version.h>
#include <Common/getHashOfLoadedBinary.h>
#include <Common/logger_useful.h>

using namespace local_engine;

using signal_function = void(int, siginfo_t *, void *);

static const size_t signal_pipe_buf_size
    = sizeof(int) + sizeof(siginfo_t) + sizeof(ucontext_t *) + sizeof(StackTrace) + sizeof(UInt32) + sizeof(void *);

static std::atomic_flag fatal_error_printed;

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_SET_SIGNAL_HANDLER;
    extern const int CANNOT_SEND_SIGNAL;
    extern const int SYSTEM_ERROR;
}
}

extern String getGitHash();

using namespace DB;
PipeFDs signal_pipe;
ALWAYS_INLINE int readFD()
{
    return signal_pipe.fds_rw[0];
}

ALWAYS_INLINE int writeFD()
{
    return signal_pipe.fds_rw[1];
}

static void addSignalHandler(const std::vector<int> & signals, signal_function handler, std::vector<int> * out_handled_signals)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_sigaction = handler;
    sa.sa_flags = SA_SIGINFO;

#if defined(OS_DARWIN)
    sigemptyset(&sa.sa_mask);
    for (auto signal : signals)
        sigaddset(&sa.sa_mask, signal);
#else
    if (sigemptyset(&sa.sa_mask))
        throw Poco::Exception("Cannot set signal handler.");

    for (auto signal : signals)
        if (sigaddset(&sa.sa_mask, signal))
            throw Poco::Exception("Cannot set signal handler.");
#endif

    for (auto signal : signals)
        if (sigaction(signal, &sa, nullptr))
            throw Poco::Exception("Cannot set signal handler.");

    if (out_handled_signals)
        std::copy(signals.begin(), signals.end(), std::back_inserter(*out_handled_signals));
}

static void writeSignalIDtoSignalPipe(int sig)
{
    auto saved_errno = errno; /// We must restore previous value of errno in signal handler.
    char buf[signal_pipe_buf_size];
    WriteBufferFromFileDescriptor out(writeFD(), signal_pipe_buf_size, buf);
    writeBinary(sig, out);
    out.finalize();
    errno = saved_errno;
}

static void call_default_signal_handler(int sig)
{
    if (SIG_ERR == signal(sig, SIG_DFL))
        throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Cannot set signal handler");

    if (0 != raise(sig))
        throw ErrnoException(ErrorCodes::CANNOT_SEND_SIGNAL, "Cannot send signal");
}

static void signalHandler(int sig, siginfo_t * info, void * context) noexcept
{
    DENY_ALLOCATIONS_IN_SCOPE;
    auto saved_errno = errno; /// We must restore previous value of errno in signal handler.

    char buf[signal_pipe_buf_size];
    DB::WriteBufferFromFileDescriptorDiscardOnFailure out(writeFD(), signal_pipe_buf_size, buf);

    const ucontext_t * signal_context = reinterpret_cast<ucontext_t *>(context);
    const StackTrace stack_trace(*signal_context);

    DB::writeBinary(sig, out);
    DB::writePODBinary(*info, out);
    DB::writePODBinary(signal_context, out);
    DB::writePODBinary(stack_trace, out);
    DB::writeBinary(static_cast<UInt32>(getThreadId()), out);
    DB::writePODBinary(DB::current_thread, out);

    out.next();

    if (sig != SIGTSTP) /// This signal is used for debugging.
    {
        /// The time that is usually enough for separate thread to print info into log.
        /// Under MSan full stack unwinding with DWARF info about inline functions takes 101 seconds in one case.
        for (size_t i = 0; i < 300; ++i)
        {
            /// We will synchronize with the thread printing the messages with an atomic variable to finish earlier.
            if (fatal_error_printed.test())
                break;

            /// This coarse method of synchronization is perfectly ok for fatal signals.
            sleepForSeconds(1);
        }
        call_default_signal_handler(sig);
    }

    errno = saved_errno;
}

/// Avoid link time dependency on DB/Interpreters - will use this function only when linked.
__attribute__((__weak__)) void
collectGlutenCrashLog(Int32 signal, UInt64 thread_id, const String & query_id, const StackTrace & stack_trace);

class SignalListener : public Poco::Runnable
{
public:
    static constexpr int StdTerminate = -1;
    static constexpr int StopThread = -2;
    static constexpr int SanitizerTrap = -3;
    SignalListener() : log(&Poco::Logger::get("SignalListener")), git_hash("getGitHash()") { }

    void run() override
    {
        static_assert(PIPE_BUF >= 512);
        static_assert(
            signal_pipe_buf_size <= PIPE_BUF,
            "Only write of PIPE_BUF to pipe is atomic and the minimal known PIPE_BUF across supported platforms is 512");
        char buf[signal_pipe_buf_size];
        ReadBufferFromFileDescriptor in(readFD(), signal_pipe_buf_size, buf);

        while (!in.eof())
        {
            int sig = 0;
            readBinary(sig, in);
            // We may log some specific signals afterward, with different log
            // levels and more info, but for completeness we log all signals
            // here at trace level.
            // Don't use strsignal here, because it's not thread-safe.
            LOG_TRACE(log, "Received signal {}", sig);

            if (sig == StopThread)
            {
                LOG_INFO(log, "Stop SignalListener thread");
                break;
            }
            else if (sig == StdTerminate)
            {
                UInt32 thread_num;
                std::string message;

                readBinary(thread_num, in);
                readBinary(message, in);

                onTerminate(message, thread_num);
            }
            else
            {
                siginfo_t info{};
                ucontext_t * context{};
                StackTrace stack_trace(NoCapture{});
                UInt32 thread_num{};
                ThreadStatus * thread_ptr{};

                if (sig != SanitizerTrap)
                {
                    readPODBinary(info, in);
                    readPODBinary(context, in);
                }

                readPODBinary(stack_trace, in);
                readBinary(thread_num, in);
                readPODBinary(thread_ptr, in);

                /// This allows to receive more signals if failure happens inside onFault function.
                /// Example: segfault while symbolizing stack trace.
                std::thread([sig, info, context, stack_trace, thread_num, thread_ptr, this]
                            { onFault(sig, info, context, stack_trace, thread_num, thread_ptr); })
                    .detach();
            }
        }
    }

private:
    Poco::Logger * log;
    std::string build_id; // TODO : Build ID
    std::string git_hash;
    std::string stored_binary_hash; // TODO: binary checksum
    void onTerminate(std::string_view /*message*/, UInt32 /*thread_num*/) const { }

    void onFault(
        int sig, const siginfo_t & info, ucontext_t * context, const StackTrace & stack_trace, UInt32 thread_num, ThreadStatus * thread_ptr)
        const
    {
        String query_id;
        String query;

        /// Send logs from this thread to client if possible.
        /// It will allow client to see failure messages directly.
        if (thread_ptr)
        {
            query_id = thread_ptr->getQueryId();
            query = thread_ptr->getQueryForLog();

            if (auto logs_queue = thread_ptr->getInternalTextLogsQueue())
                CurrentThread::attachInternalTextLogsQueue(logs_queue, LogsLevel::trace);
        }
        std::string signal_description = "Unknown signal";

        /// Some of these are not really signals, but our own indications on failure reason.
        if (sig == StdTerminate)
            signal_description = "std::terminate";
        else if (sig == SanitizerTrap)
            signal_description = "sanitizer trap";
        else if (sig >= 0)
            signal_description = strsignal(sig); // NOLINT(concurrency-mt-unsafe) // it is not thread-safe but ok in this context

        LOG_FATAL(log, "########################################");

        if (query_id.empty())
        {
            LOG_FATAL(
                log,
                "(version {}{}, build id: {}, git hash: {}) (from thread {}) (no query) Received signal {} ({})",
                VERSION_STRING,
                VERSION_OFFICIAL,
                build_id,
                git_hash,
                thread_num,
                signal_description,
                sig);
        }
        else
        {
            LOG_FATAL(
                log,
                "(version {}{}, build id: {}, git hash: {}) (from thread {}) (query_id: {}) (query: {}) Received signal {} ({})",
                VERSION_STRING,
                VERSION_OFFICIAL,
                build_id,
                git_hash,
                thread_num,
                query_id,
                query,
                signal_description,
                sig);
        }
        String error_message;

        if (sig != SanitizerTrap)
            error_message = signalToErrorMessage(sig, info, *context);
        else
            error_message = "Sanitizer trap.";

        LOG_FATAL(log, fmt::runtime(error_message));

        /// Write symbolized stack trace line by line for better grep-ability.
        stack_trace.toStringEveryLine([this](std::string_view s) { LOG_FATAL(log, fmt::runtime(s)); });

#if defined(OS_LINUX)
        /// Write information about binary checksum. It can be difficult to calculate, so do it only after printing stack trace.
        /// TODO: Please keep the below log messages in-sync with the ones in ~programs/server/Server.cpp~

        if (stored_binary_hash.empty())
        {
            LOG_FATAL(log, "Integrity check of the executable skipped because the reference checksum could not be read.");
        }
        else
        {
            String calculated_binary_hash = getHashOfLoadedBinaryHex();
            if (calculated_binary_hash == stored_binary_hash)
            {
                LOG_FATAL(log, "Integrity check of the executable successfully passed (checksum: {})", calculated_binary_hash);
            }
            else
            {
                LOG_FATAL(
                    log,
                    "Calculated checksum of the executable ({0}) does not correspond"
                    " to the reference checksum stored in the executable ({1})."
                    " This may indicate one of the following:"
                    " - the executable was changed just after startup;"
                    " - the executable was corrupted on disk due to faulty hardware;"
                    " - the loaded executable was corrupted in memory due to faulty hardware;"
                    " - the file was intentionally modified;"
                    " - a logical error in the code.",
                    calculated_binary_hash,
                    stored_binary_hash);
            }
        }
#endif
        /// FIXME: Write crash to system.crash_log table if available.
        if (collectGlutenCrashLog)
            collectGlutenCrashLog(sig, thread_num, query_id, stack_trace);

        ///TODO: Send crash report to developers (if configured)
        if (sig != SanitizerTrap)
        {
            /// TODO: SentryWriter::onFault(sig, error_message, stack_trace);

            /// TODO:  Advice the user to send it manually.
        }
        /// ClickHouse Keeper does not link to some part of Settings.
#ifndef CLICKHOUSE_PROGRAM_STANDALONE_BUILD
        /// List changed settings.
        if (!query_id.empty())
        {
            ContextPtr query_context = thread_ptr->getQueryContext();
            if (query_context)
            {
                String changed_settings = query_context->getSettingsRef().toString();

                if (changed_settings.empty())
                    LOG_FATAL(log, "No settings were changed");
                else
                    LOG_FATAL(log, "Changed settings: {}", changed_settings);
            }
        }
#endif
        /// When everything is done, we will try to send these error messages to client.
        if (thread_ptr)
            thread_ptr->onFatalError();

        fatal_error_printed.test_and_set();
    }
};

namespace local_engine
{

struct SignalHandler::Impl
{
    /// A thread that acts on HUP and USR1 signal (close logs).
    Poco::Thread signal_listener_thread;
    std::unique_ptr<Poco::Runnable> signal_listener;
    std::vector<int> handled_signals;

    /// initialize termination process and signal handlers
    void initializeTerminationAndSignalProcessing()
    {
        addSignalHandler({SIGABRT, SIGSEGV, SIGILL, SIGBUS, SIGSYS, SIGFPE, SIGPIPE, SIGTSTP, SIGTRAP}, signalHandler, &handled_signals);

        /// TODO:: Set up Poco ErrorHandler for Poco Threads.
        // static KillingErrorHandler killing_error_handler;
        // Poco::ErrorHandler::set(&killing_error_handler);

        signal_pipe.setNonBlockingWrite();
        signal_pipe.tryIncreaseSize(1 << 20);
        signal_listener = std::make_unique<SignalListener>();
        signal_listener_thread.start(*signal_listener);
    }

    ~Impl()
    {
        writeSignalIDtoSignalPipe(SignalListener::StopThread);
        signal_listener_thread.join();

        /// Reset signals to SIG_DFL to avoid trying to write to the signal_pipe that will be closed after.
        for (int sig : handled_signals)
            if (SIG_ERR == signal(sig, SIG_DFL))
            {
                try
                {
                    throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Cannot set signal handler");
                }
                catch (ErrnoException &)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
            }
        signal_pipe.close();
    }
};

void SignalHandler::init()
{
    if (pimpl)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The SignalHandler is initialized twice");

    // getenv is not thread-safe, but it's fine here.
    Poco::Logger * log = &Poco::Logger::get("SignalHandler");
    const char * preload = std::getenv("LD_PRELOAD");

    bool find_libjsig = preload != nullptr && std::string_view(preload).find("libjsig.so") != std::string_view::npos;

    if (find_libjsig)
    {
        LOG_WARNING(log, "LD_PRELOAD is {}", preload);
        updatePHDRCache();
        pimpl = std::make_unique<Impl>();
        pimpl->initializeTerminationAndSignalProcessing();
    }
    else
    {
        LOG_WARNING(log, "LD_PRELOAD is not set, SignalHandler is disabled");
    }
}

SignalHandler::SignalHandler() = default;
SignalHandler::~SignalHandler() = default;
}
