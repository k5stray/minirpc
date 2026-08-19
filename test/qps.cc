#include <iostream>
#include <cstdio>
#include <atomic>
#include <google/protobuf/stubs/common.h>
#include "../src/rpc/rpc_stub.h"
#include "../proto/rpc_proto.pb.h"
#include "../src/util/rpc_util.h"
#include "../src/util/log_util.h"

using Clock = std::chrono::steady_clock;
using TimePoint = std::chrono::time_point<Clock>;

std::mutex g_response_time_mtx;
std::vector<long long> g_response_times;
long long g_total_requests = 0;
std::atomic<int> g_fail_count(0);
std::atomic<int> g_sucess_count(0);
std::atomic<int> g_async_count(0);
std::condition_variable g_async_wait;
std::mutex g_async_mtx;
TimePoint g_end_time;
TimePoint g_start_time;

static void echo_client_async(RpcStub &stub, int requests_per_thread, int total) 
{
    for (int i = 0; i < requests_per_thread; ++i) {
        auto send_time = Clock::now();

        minirpc::DemoReq req;
        long long reqid = get_serial_id();
        std::string message = "async";
        req.set_serialid(reqid);
        req.set_servername("EchoService");
        req.set_funcname("echo");
        req.set_param(message);
        stub.call_async(req, [send_time, total, reqid](minirpc::DemoResp resp){
            if (strcmp(resp.result().c_str(), "async") == 0) {
                g_sucess_count.fetch_add(1, std::memory_order_relaxed);
            } else {
                g_fail_count.fetch_add(1, std::memory_order_relaxed);
            }

            auto recv_time = Clock::now();
            long long rt = std::chrono::duration_cast<std::chrono::microseconds>(
                recv_time - send_time
            ).count();

            std::lock_guard<std::mutex> lock(g_response_time_mtx);
            g_response_times.push_back(rt);
            g_total_requests++;
            g_async_count++;
            if (g_async_count.load() == total) {
                g_async_wait.notify_one();
            }
            return 0;
        });

    }
}

static void echo_client_sync(RpcStub &stub, int requests_per_thread)
{
    for (int i = 0; i < requests_per_thread; ++i) {
        auto send_time = Clock::now();

        minirpc::DemoReq req;
        long long reqid = get_serial_id();
        std::string message = " sync";
        req.set_serialid(reqid);
        req.set_servername("EchoService");
        req.set_funcname("echo");
        req.set_param(message);
        minirpc::DemoResp resp;
        if (stub.call(req, resp)) {
            if (strcmp(resp.result().c_str(), message.c_str()) == 0) {
                g_sucess_count.fetch_add(1, std::memory_order_relaxed);
            } else {
                g_fail_count.fetch_add(1, std::memory_order_relaxed);
            }
        } else {
            g_fail_count.fetch_add(1, std::memory_order_relaxed);
            std::cerr << "####rpc call fail!!" << std::endl;
            break;
        }

        auto recv_time = Clock::now();
        long long rt = std::chrono::duration_cast<std::chrono::microseconds>(
            recv_time - send_time
        ).count();

        std::lock_guard<std::mutex> lock(g_response_time_mtx);
        g_response_times.push_back(rt);
        g_total_requests++;
    }
}

static long long calculate_percentile(std::vector<long long>& data, double percentile)
{
    if (data.empty()) return 0;
    std::sort(data.begin(), data.end());
    int index = static_cast<int>(data.size() * percentile);

    index = std::min(index, static_cast<int>(data.size()) - 1);
    return data[index];
}

static int qps_count()
{
    double total_seconds = std::chrono::duration_cast<std::chrono::microseconds>(
        g_end_time - g_start_time
    ).count() / 1000000.0;

    if (g_response_times.empty()) {
        std::cerr << "No valid total_requests completed!" << std::endl;
        return -1;
    }

    long long total_rt = 0;
    for (long long rt : g_response_times) {
        total_rt += rt;
    }
    double avg_rt = static_cast<double>(total_rt) / g_response_times.size();

    double qps = g_total_requests / total_seconds;

    long long p50 = calculate_percentile(g_response_times, 0.5);
    long long p90 = calculate_percentile(g_response_times, 0.9);
    long long p99 = calculate_percentile(g_response_times, 0.99);

    std::cout << "----------------------------------------------" << std::endl;
    std::cout << " requests: total(" << g_total_requests << ") pass(" << g_sucess_count<< ") fail(" << g_fail_count << ")" << std::endl;
    std::cout << " time: " << total_seconds << " s" << std::endl;
    std::cout << " QPS: " << qps << " req/s" << std::endl;
    std::cout << " Avg RT: " << (avg_rt / 1000.0) << " ms" << std::endl;
    std::cout << " P50 RT: " << (p50 / 1000.0) << " ms" << std::endl;
    std::cout << " P90 RT: " << (p90 / 1000.0) << " ms" << std::endl;
    std::cout << " P99 RT: " << (p99 / 1000.0) << " ms" << std::endl;
    std::cout << "**********************************************" << std::endl;
    std::cout << std::endl;
    return 0;
}

static int qps_test(RpcStub &stub, int thread_num, int req_per_thread = 100000)
{

    int total_requests = thread_num * req_per_thread;

    std::cout << "=====minirpc QPS call [sync] testing....=====" << std::endl;
    std::cout << " #Thread numbers:      " << thread_num << std::endl;
    std::cout << " #Requests per thread: " << req_per_thread << std::endl;
    std::cout << " #Total Requests:      " << total_requests << std::endl;

    g_response_times.clear();
    g_sucess_count = 0;
    g_fail_count = 0;
    g_total_requests = 0;
    g_async_count = 0;

    /* --------async call----------- */
    g_start_time = Clock::now();

    std::vector<std::thread> threads;
    for (int i = 0; i < thread_num; ++i) {
        threads.emplace_back(echo_client_async, std::ref(stub), req_per_thread, total_requests);
    }

    for (auto& t : threads) {
        t.join();
    }

    std::unique_lock<std::mutex> mtx(g_async_mtx);
    g_async_wait.wait(mtx, [total_requests](){
        return g_async_count.load() == total_requests;
    });

    g_end_time = Clock::now();
    qps_count();
    /* --------async call----------- */

    std::cout << "=====minirpc QPS call [sync] testing....=====" << std::endl;
    std::cout << " #thread number:       " << thread_num << std::endl;
    std::cout << " #Requests per thread: " << req_per_thread << std::endl;
    std::cout << " #Total Requests:      " << total_requests << std::endl;

    g_fail_count = 0;
    g_sucess_count = 0;
    g_total_requests = 0;
    threads.clear();
    g_response_times.clear();

    /* --------sync call----------- */
    g_start_time = Clock::now();

    for (int i = 0; i < thread_num; ++i) {
        threads.emplace_back(echo_client_sync, std::ref(stub), req_per_thread);
    }

    for (auto& t : threads) {
        t.join();
    }

    g_end_time = Clock::now();

    qps_count();
    /* --------sync call done----------- */

    return 0;
}

int main()
{
    LOG_INIT("./logs", LOG_DEBUG);
    RpcStub stub;
    if(!stub.init())
    {
        std::cerr << "connect failed" << std::endl;
        return -1;
    }

    // qps_test(stub, 10, 2000000);
    qps_test(stub, 10);

    stub.stop();
    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
