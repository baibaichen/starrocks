//
// Created by chang on 22-12-9.
//

#include "agent/agent_server.h"
#include "butil/file_util.h"
#include "common/config.h"
#include "common/logging.h"
#include "exec/pipeline/query_context.h"
#include "fs/fs.h"
#include "gen_cpp/doris_internal_service.pb.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/mem_tracker.h"
#include "runtime/memory/mem_chunk_allocator.h"
#include "runtime/time_types.h"
#include "runtime/user_function_cache.h"
#include "runtime/plan_fragment_executor.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/update_manager.h"
#include "service/brpc.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/logging.h"
#include "util/mem_info.h"
#include "util/timezone_utils.h"
#include "util/thrift_util.h"

namespace starrocks {
    static void _thrift_output(const char* x) {
        VLOG_QUERY << "thrift internal message: " << x;
    }
} // namespace starrocks

class mock_internal: public doris::PBackendService {
    void transmit_chunk(google::protobuf::RpcController* cntl_base,
                        const starrocks::PTransmitChunkParams* request, starrocks::PTransmitChunkResult* response,
                        google::protobuf::Closure* done) override;
};

void mock_internal::transmit_chunk(google::protobuf::RpcController *cntl_base,
                                   const starrocks::PTransmitChunkParams *request,
                                   starrocks::PTransmitChunkResult *response, google::protobuf::Closure *done) {
    starrocks::Status st;
    st.to_protobuf(response->mutable_status());
    done->Run();
}

namespace brpc {

    DECLARE_uint64(max_body_size);
    DECLARE_int64(socket_max_unwritten_bytes);

} // namespace brpc
std::atomic<bool> k_exit = false;
std::atomic<bool> k_be_started = false;
std::unique_ptr<std::thread> _mock_backend_thread;
std::future<void> _finish_future;
std::promise<void> _finish_promise;

void mock_be(){

    // 2. Start brpc services.
    brpc::FLAGS_max_body_size = starrocks::config::brpc_max_body_size;
    brpc::FLAGS_socket_max_unwritten_bytes = starrocks::config::brpc_socket_max_unwritten_bytes;

    brpc::Server brpc_server;

    mock_internal mock;
    brpc_server.AddService(&mock, brpc::SERVER_DOESNT_OWN_SERVICE);
    brpc::ServerOptions options;

    if (brpc_server.Start(starrocks::config::brpc_port, &options) != 0) {
        LOG(ERROR) << "BRPC service did not start correctly, exiting";
        starrocks::shutdown_logging();
        exit(1);
    }

    k_be_started.store(true);

    LOG(INFO) << "mock BRPC service start correctly";

    while (!k_exit.load()) {
        sleep(1);
    }

    brpc_server.Stop(0);
    brpc_server.Join();

}

void start_be() {
    _mock_backend_thread = std::make_unique<std::thread>(mock_be);
    while (!k_be_started.load()) {
        sleep(1);
    }
}

namespace starrocks {

    Status runStatus;

    void onFinish(PlanFragmentExecutor *exec) {

        k_exit.store(true);
        _finish_promise.set_value();
        runStatus = exec->status();
        if(runStatus.ok() && !exec->is_done()) {
         runStatus = Status::InternalError("PlanFragmentExecutor is not done, but status is ok");
        }
    }

    Status run(ExecEnv* _exec_env, const std::string& fname ) {
        auto fs = starrocks::FileSystem::Default();
        auto rfile = *fs->new_random_access_file(fname);
        ASSIGN_OR_RETURN(uint64_t file_size, rfile->get_size());

        raw::RawVector<uint8_t> buff;
        buff.resize(file_size);
        RETURN_IF_ERROR(rfile->read_fully(buff.data(), file_size));
        CHECK(buff.size() == file_size);

        TExecPlanFragmentParams fromJson;
        uint32_t len = buff.size();
        RETURN_IF_ERROR(deserialize_thrift_msg(buff.data(), &len, TProtocolType::JSON, &fromJson));

        CHECK(fromJson.__isset.desc_tbl);
        _finish_future = _finish_promise.get_future();
        RETURN_IF_ERROR(_exec_env->fragment_mgr()->exec_plan_fragment(fromJson, onFinish));
        auto wait_state = _finish_future.wait_for(std::chrono::seconds(1000));
        RETURN_IF(std::future_status::ready != wait_state, Status::TimedOut("run segment timeout"));
        return runStatus;
    }
}

int main(int argc, char** argv) {

    if (getenv("STARROCKS_HOME") == nullptr) {
        fprintf(stderr, "you need set STARROCKS_HOME environment variable.\n");
        exit(-1);
    }

    if (getenv("TCMALLOC_HEAP_LIMIT_MB") == nullptr) {
        fprintf(stderr,
                "Environment variable TCMALLOC_HEAP_LIMIT_MB is not set,"
                " maybe you forgot to replace bin directory\n");
        exit(-1);
    }

    // Open pid file, obtain file lock and save pid.
    std::string pid_file = string(getenv("PID_DIR")) + "/run_segment.pid";
    int fd = open(pid_file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (fd < 0) {
        fprintf(stderr, "fail to create pid file.");
        exit(-1);
    }

    std::string pid = std::to_string((long)getpid());
    pid += "\n";
    size_t length = write(fd, pid.c_str(), pid.size());
    if (length != pid.size()) {
        fprintf(stderr, "fail to save pid into pid file.");
        exit(-1);
    }

    // Descriptor will be leaked if failing to close fd.
    if (::close(fd) < 0) {
        fprintf(stderr, "failed to close fd of pidfile.");
        exit(-1);
    }

    // config
    std::string conffile = std::string(getenv("STARROCKS_HOME")) + "/conf/be.conf";
    if (!starrocks::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    // Add logger for thrift internal.
    apache::thrift::GlobalOutput.setOutputFunction(starrocks::_thrift_output);
    starrocks::init_glog("run_segment", true);
    starrocks::CpuInfo::init();
    starrocks::DiskInfo::init();
    starrocks::MemInfo::init();
    starrocks::UserFunctionCache::instance()->init(starrocks::config::user_function_dir);

    starrocks::date::init_date_cache();
    starrocks::TimezoneUtils::init_time_zones();

    auto* exec_env = starrocks::ExecEnv::GetInstance();
    EXIT_IF_ERROR(exec_env->init_mem_tracker());

    // bool without_storage = true;
    // Init and open storage engine.
    std::vector<starrocks::StorePath> paths;
    starrocks::EngineOptions options;

    options.store_paths = paths;
    options.backend_uid = starrocks::UniqueId::gen_uid();
    options.compaction_mem_tracker = exec_env->compaction_mem_tracker();
    options.update_mem_tracker = exec_env->update_mem_tracker();
    options.conf_path = string(getenv("STARROCKS_HOME")) + "/conf/";
    starrocks::StorageEngine* engine = nullptr;
    auto st = starrocks::DummyStorageEngine::open(options, &engine);
    if (!st.ok()) {
        LOG(FATAL) << "fail to open StorageEngine, res=" << st.get_error_msg();
        exit(-1);
    }

    // Init exec env.
    starrocks::ExecEnv::init(exec_env, paths);

    start_be();

    {
        std::string fname = "/home/chang/test/20221208223008.053.new.json";
        starrocks::Status status = starrocks::run(exec_env, fname);
        if (!status.ok()) {
            LOG(WARNING) << status.get_error_msg();
        }
    }

    k_exit.store(true);
    _mock_backend_thread->join();

    // delete engine
    starrocks::StorageEngine::instance()->stop();

    // destroy exec env
    starrocks::ExecEnv::destroy(exec_env);
    starrocks::shutdown_logging();
    return 0;
}