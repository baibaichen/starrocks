//
// Created by chang on 11/25/22.
//

#include <gtest/gtest.h>

#include <memory>
#include <mutex>
#include <random>
#include <functional>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/exchange/local_exchange.h"
#include "exec/pipeline/exchange/local_exchange_sink_operator.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/scan/connector_scan_operator.h"
#include "exec/connector_scan_node.h"
#include "gen_cpp/InternalService_types.h"
#include "gtest/gtest.h"
#include "gutil/map_util.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/plan_fragment_executor.h"
#include "storage/storage_engine.h"
#include "util/defer_op.h"
#include "util/disk_info.h"
#include "util/mem_info.h"
#include "util/thrift_util.h"

namespace starrocks{


    class FragmentScanTest : public ::testing::Test {
        void SetUp() override {
            _exec_env = ExecEnv::GetInstance();
        }

        void TearDown() override {}
    protected:
        void init();
        void init_desc_tbl(const std::vector<TypeDescriptor>& types);
        void init_plan_exec_params(const std::vector<TypeDescriptor>& types);
        void init_plan_fragment();
        void onFinish(PlanFragmentExecutor* exec);
        static void _configPlanNode(TPlanNode& planNode);
        static std::vector<TScanRangeParams> _createCSVScanRanges(const std::vector<TypeDescriptor>& types,
                                                              const string& multi_row_delimiter = "\n",
                                                              const string& multi_column_separator = "|");

        static const TTupleId tupleId = 0;
        static const TPlanNodeId scanNodeId = 1;
    private:
        ExecEnv* _exec_env = nullptr;
        TExecPlanFragmentParams _request;
        std::future<void> _finish_future;
        std::promise<void> _finish_promise;
    };

    void FragmentScanTest::init_desc_tbl(const std::vector<TypeDescriptor>& types) {
        /// Init DescriptorTable
        TDescriptorTableBuilder desc_tbl_builder;
        TTupleDescriptorBuilder tuple_desc_builder;
        for (auto& t : types) {
            TSlotDescriptorBuilder slot_desc_builder;
            slot_desc_builder.type(t).length(t.len).precision(t.precision).scale(t.scale).nullable(true);
            tuple_desc_builder.add_slot(slot_desc_builder.build());
        }
        tuple_desc_builder.build(&desc_tbl_builder);
        _request.__set_desc_tbl(desc_tbl_builder.desc_tbl());
    }


    void FragmentScanTest::init() {
        std::vector<TypeDescriptor> types;
        types.emplace_back(TYPE_INT);
        types.emplace_back(TYPE_DOUBLE);
        types.emplace_back(TYPE_VARCHAR);
        types.emplace_back(TYPE_DATE);
        types.emplace_back(TYPE_VARCHAR);
        init_desc_tbl(types);
        init_plan_fragment();
        init_plan_exec_params(types);
        _finish_future = _finish_promise.get_future();
    }

    void FragmentScanTest::init_plan_fragment() {
        TPlanFragment fragment;
        _configPlanNode(fragment.plan.nodes.emplace_back());
        TDataSink sink;
        TMemoryScratchSink memoryScratchSink;
        sink.__set_type(TDataSinkType::MEMORY_SCRATCH_SINK);
        sink.__set_memory_scratch_sink(memoryScratchSink);
        fragment.__set_output_sink(sink);
        _request.__set_fragment(std::move(fragment));
    }

    void FragmentScanTest::_configPlanNode(TPlanNode& planNode) {
        std::vector<::starrocks::TTupleId> tuple_ids{tupleId};
        std::vector<bool> nullable_tuples{true};

        planNode.__set_node_id(scanNodeId);
        planNode.__set_node_type(TPlanNodeType::FILE_SCAN_NODE);
        planNode.__set_row_tuples(tuple_ids);
        planNode.__set_nullable_tuples(nullable_tuples);
        planNode.__set_use_vectorized(true);
        planNode.__set_limit(-1);

//        TConnectorScanNode connector_scan_node;
//        connector_scan_node.connector_name = connector::Connector::FILE;
//        planNode.__set_connector_scan_node(connector_scan_node);

        TFileScanNode fileScanNode;
        fileScanNode.__set_tuple_id(tupleId);
        planNode.__set_file_scan_node(fileScanNode);
    }

    void FragmentScanTest::onFinish(PlanFragmentExecutor *exec) {
        _finish_promise.set_value();
        ASSERT_TRUE(exec->is_done());
        ASSERT_TRUE(exec->status().ok());
    }

    std::vector<TScanRangeParams>
    FragmentScanTest::_createCSVScanRanges(const vector<TypeDescriptor> &types, const string &multi_row_delimiter,
                                           const string &multi_column_separator) {
        std::string _file = "./be/test/exec/test_data/csv_scanner/csv_file1";

        TBrokerScanRangeParams params;
        params.__set_multi_row_delimiter(multi_row_delimiter);
        params.__set_multi_column_separator(multi_column_separator);
        params.strict_mode = true;
        params.dest_tuple_id = tupleId;
        params.src_tuple_id = tupleId;

        for (int i = 0; i < types.size(); i++) {
            params.expr_of_dest_slot[i] = TExpr();
            params.expr_of_dest_slot[i].nodes.emplace_back(TExprNode());
            params.expr_of_dest_slot[i].nodes[0].__set_type(types[i].to_thrift());
            params.expr_of_dest_slot[i].nodes[0].__set_node_type(TExprNodeType::SLOT_REF);
            params.expr_of_dest_slot[i].nodes[0].__set_is_nullable(true);
            params.expr_of_dest_slot[i].nodes[0].__set_slot_ref(TSlotRef());
            params.expr_of_dest_slot[i].nodes[0].slot_ref.__set_slot_id(i);
        }

        for (int i = 0; i < types.size(); i++) {
            params.src_slot_ids.emplace_back(i);
        }

        std::vector<TBrokerRangeDesc> ranges;
        TBrokerRangeDesc& range = ranges.emplace_back();
        range.__set_path(_file);
        range.__set_start_offset(0);
        range.__set_num_of_columns_from_file(types.size());

        TBrokerScanRange broker_scan_range;
        broker_scan_range.__set_params(params);
        broker_scan_range.__set_ranges(ranges);

        TScanRange scan_range;
        scan_range.__set_broker_scan_range(broker_scan_range);

        TScanRangeParams param;
        param.__set_scan_range(scan_range);

        return std::vector<TScanRangeParams>{param};
    }

    void FragmentScanTest::init_plan_exec_params(const vector<TypeDescriptor> &types) {
        TPlanNodeId x = scanNodeId;
        TPlanFragmentExecParams params;
        params.per_node_scan_ranges[x] = _createCSVScanRanges(types);
        _request.__set_params(params);
    }


    TEST_F(FragmentScanTest, CSVBasic) {
        init();
        FragmentMgr::FinishCallback cb = [this](auto && PH1) { onFinish(std::forward<decltype(PH1)>(PH1)); };
        Status status = _exec_env->fragment_mgr()->exec_plan_fragment(_request, cb);
        ASSERT_EQ(std::future_status::ready, _finish_future.wait_for(std::chrono::seconds(15)));
        ASSERT_TRUE(status.ok());
    }
}