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
#pragma once
#include <Interpreters/Context_fwd.h>
#include <base/types.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool_fwd.h>

namespace local_engine
{

using JobId = String;
using Task = std::function<void()>;

class Job
{
    friend class JobScheduler;
public:
    explicit Job(const JobId& id)
        : id(id)
    {
    }

    void addTask(Task&& task)
    {
        tasks.emplace_back(task);
    }

private:
    JobId id;
    std::vector<Task> tasks;
};



struct JobSatus
{
    enum Status
    {
        RUNNING,
        FINISHED,
        FAILED
    };
    Status status;
    std::vector<String> messages;

    static JobSatus success()
    {
        return JobSatus{FINISHED};
    }

    static JobSatus running()
    {
        return JobSatus{RUNNING};
    }

    static JobSatus failed(const std::vector<std::string> & messages)
    {
        return JobSatus{FAILED, messages};
    }
};

struct TaskResult
{
    enum Status
    {
        SUCCESS,
        FAILED,
        RUNNING
    };
    Status status = RUNNING;
    String message;
};

class JobContext
{
public:
    Job job;
    std::unique_ptr<std::atomic_uint32_t> remain_tasks = std::make_unique<std::atomic_uint32_t>();
    std::vector<TaskResult> task_results;

    bool isFinished()
    {
        return remain_tasks->load(std::memory_order::relaxed) == 0;
    }
};

class JobScheduler
{
public:
    static JobScheduler & instance()
    {
        static JobScheduler global_job_scheduler;
        return global_job_scheduler;
    }

    static void initialize(const DB::ContextPtr & context);

    JobId scheduleJob(Job&& job);

    std::optional<JobSatus> getJobSatus(const JobId& job_id);

    void cleanupJob(const JobId& job_id);

    void addFinishedJob(const JobId& job_id);

    void cleanFinishedJobs();
    ~JobScheduler();
private:
    JobScheduler();
    std::unique_ptr<ThreadPool> thread_pool;
    std::unordered_map<JobId, JobContext> job_details;
    std::mutex job_details_mutex;

    std::vector<std::pair<JobId, Stopwatch>> finished_job;
    std::mutex finished_job_mutex;
    LoggerPtr logger = getLogger("JobScheduler");
};
}
