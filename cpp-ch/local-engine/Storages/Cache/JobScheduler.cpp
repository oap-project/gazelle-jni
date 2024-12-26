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


#include "JobScheduler.h"

#include <Interpreters/Context.h>
#include <Common/GlutenConfig.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
}

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
extern const Metric LocalThreadScheduled;
}

namespace local_engine
{
std::shared_ptr<JobScheduler> global_job_scheduler = nullptr;

JobScheduler::JobScheduler() = default;
JobScheduler::~JobScheduler() = default;

void JobScheduler::initialize(const DB::ContextPtr & context)
{
    auto config = GlutenJobSchedulerConfig::loadFromContext(context);
    instance().thread_pool = std::make_unique<ThreadPool>(
        CurrentMetrics::LocalThread,
        CurrentMetrics::LocalThreadActive,
        CurrentMetrics::LocalThreadScheduled,
        config.job_scheduler_max_threads,
        0,
        0);

}

JobId JobScheduler::scheduleJob(Job&& job)
{
    cleanFinishedJobs();
    if (job_details.contains(job.id))
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "job {} exists.", job.id);
    }
    size_t task_num = job.tasks.size();
    auto job_id = job.id;
    std::vector<TaskResult> task_results;
    task_results.reserve(task_num);
    JobContext job_context = {std::move(job), std::make_unique<std::atomic_uint32_t>(task_num), std::move(task_results)};
    {
        std::lock_guard lock(job_details_mutex);
        job_details.emplace(job_id, std::move(job_context));
    }
    LOG_INFO(logger, "schedule job {}", job_id);

    auto & job_detail = job_details.at(job_id);

    for (auto & task : job_detail.job.tasks)
    {
        job_detail.task_results.emplace_back(TaskResult());
        auto & task_result = job_detail.task_results.back();
        thread_pool->scheduleOrThrow(
            [&]()
            {
                SCOPE_EXIT({
                    job_detail.remain_tasks->fetch_sub(1, std::memory_order::acquire);
                    if (job_detail.isFinished())
                    {
                        addFinishedJob(job_detail.job.id);
                    }
                });
                try
                {
                    task();
                    task_result.status = TaskResult::Status::SUCCESS;
                }
                catch (std::exception & e)
                {
                    task_result.status = TaskResult::Status::FAILED;
                    task_result.message = e.what();
                }
            });
    }
    return job_id;
}

std::optional<JobSatus> JobScheduler::getJobSatus(const JobId & job_id)
{
    if (!job_details.contains(job_id))
    {
        return std::nullopt;
    }
    std::optional<JobSatus> res;
    auto & job_context = job_details.at(job_id);
    if (job_context.isFinished())
    {
        std::vector<String> messages;
        for (auto & task_result : job_context.task_results)
        {
            if (task_result.status == TaskResult::Status::FAILED)
            {
                messages.push_back(task_result.message);
            }
        }
        if (messages.empty())
            res = JobSatus::success();
        else
            res= JobSatus::failed(messages);
    }
    else
        res = JobSatus::running();
    return res;
}

void JobScheduler::cleanupJob(const JobId & job_id)
{
    LOG_INFO(logger, "clean job {}", job_id);
    job_details.erase(job_id);
}

void JobScheduler::addFinishedJob(const JobId & job_id)
{
    std::lock_guard lock(finished_job_mutex);
    auto job = std::make_pair(job_id, Stopwatch());
    finished_job.emplace_back(job);
}

void JobScheduler::cleanFinishedJobs()
{
    std::lock_guard lock(finished_job_mutex);
    for (auto it = finished_job.begin(); it != finished_job.end();)
    {
        // clean finished job after 5 minutes
        if (it->second.elapsedSeconds() > 60 * 5)
        {
            cleanupJob(it->first);
            it = finished_job.erase(it);
        }
        else
            ++it;
    }
}
}