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

#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/Aggregator.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Parser/RelMetric.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/CustomStorageMergeTree.h>
#include <Storages/SourceFromJavaIter.h>
#include <base/types.h>
#include <substrait/plan.pb.h>
#include <Common/BlockIterator.h>

namespace local_engine
{

static const std::map<std::string, std::string> SCALAR_FUNCTIONS
    = {{"is_not_null", "isNotNull"},
       {"is_null", "isNull"},
       {"gte", "greaterOrEquals"},
       {"gt", "greater"},
       {"lte", "lessOrEquals"},
       {"lt", "less"},
       {"equal", "equals"},

       {"and", "and"},
       {"or", "or"},
       {"not", "not"},
       {"xor", "xor"},

       {"extract", ""},
       {"cast", "CAST"},
       {"alias", "alias"},

       /// datetime functions
       {"get_timestamp", "parseDateTimeInJodaSyntaxOrNull"}, // for spark function: to_date/to_timestamp
       {"quarter", "toQuarter"},
       {"to_unix_timestamp", "parseDateTimeInJodaSyntaxOrNull"},
       //{"unix_timestamp", "toUnixTimestamp"},
       {"date_format", "formatDateTimeInJodaSyntax"},
       {"timestamp_add", "timestamp_add"},


       /// arithmetic functions
       {"subtract", "minus"},
       {"multiply", "multiply"},
       {"add", "plus"},
       {"divide", "divide"},
       {"positive", "identity"},
       {"negative", "negate"},
       {"modulus", "modulo"},
       {"pmod", "pmod"},
       {"abs", "abs"},
       {"ceil", "ceil"},
       {"round", "roundHalfUp"},
       {"bround", "roundBankers"},
       {"exp", "exp"},
       {"power", "power"},
       {"cos", "cos"},
       {"cosh", "cosh"},
       {"sin", "sin"},
       {"sinh", "sinh"},
       {"tan", "tan"},
       {"tanh", "tanh"},
       {"acos", "acos"},
       {"asin", "asin"},
       {"atan", "atan"},
       {"atan2", "atan2"},
       {"asinh", "asinh"},
       {"acosh", "acosh"},
       {"atanh", "atanh"},
       {"bitwise_not", "bitNot"},
       {"bitwise_and", "bitAnd"},
       {"bitwise_or", "bitOr"},
       {"bitwise_xor", "bitXor"},
       {"bit_get", "bitTest"},
       {"bit_count", "bitCount"},
       {"sqrt", "sqrt"},
       {"cbrt", "cbrt"},
       {"degrees", "degrees"},
       {"e", "e"},
       {"pi", "pi"},
       {"hex", "hex"},
       {"unhex", "unhex"},
       {"hypot", "hypot"},
       {"sign", "sign"},
       {"radians", "radians"},
       {"greatest", "sparkGreatest"},
       {"least", "sparkLeast"},
       {"shiftleft", "bitShiftLeft"},
       {"shiftright", "bitShiftRight"},
       {"check_overflow", "checkDecimalOverflowSpark"},
       {"rand", "randCanonical"},
       {"isnan", "isNaN"},
       {"bin", "sparkBin"},
       {"rint", "sparkRint"},

       /// string functions
       {"like", "like"},
       {"not_like", "notLike"},
       {"starts_with", "startsWithUTF8"},
       {"ends_with", "endsWithUTF8"},
       {"contains", "countSubstrings"},
       {"substring", "substringUTF8"},
       {"substring_index", "substringIndexUTF8"},
       {"lower", "lowerUTF8"},
       {"upper", "upperUTF8"},
       {"trim", ""}, // trimLeft or trimLeftSpark, depends on argument size
       {"ltrim", ""}, // trimRight or trimRightSpark, depends on argument size
       {"rtrim", ""}, // trimBoth or trimBothSpark, depends on argument size
       {"strpos", "positionUTF8"},
       {"replace", "replaceAll"},
       {"regexp_replace", "replaceRegexpAll"},
       {"regexp_extract_all", "regexpExtractAllSpark"},
       {"rlike", "match"},
       {"ascii", "ascii"},
       {"split", "splitByRegexp"},
       {"concat_ws", "concat_ws"},
       {"base64", "base64Encode"},
       {"unbase64", "base64Decode"},
       {"lpad", "leftPadUTF8"},
       {"rpad", "rightPadUTF8"},
       {"reverse", ""}, /// dummy mapping
       {"translate", "translateUTF8"},
       {"repeat", "repeat"},
       {"space", "space"},
       {"initcap", "initcapUTF8"},
       {"conv", "sparkConv"},
       {"uuid", "generateUUIDv4"},
       {"levenshteinDistance", "editDistanceUTF8"},

       /// hash functions
       {"crc32", "CRC32"},
       {"murmur3hash", "sparkMurmurHash3_32"},
       {"xxhash64", "sparkXxHash64"},

       // in functions
       {"in", "in"},

       // null related functions
       {"coalesce", "coalesce"},

       // date or datetime functions
       {"from_unixtime", "fromUnixTimestampInJodaSyntax"},
       {"date_add", "addDays"},
       {"date_sub", "subtractDays"},
       {"datediff", "dateDiff"},
       {"second", "toSecond"},
       {"add_months", "addMonths"},
       {"date_trunc", "dateTrunc"},
       {"floor_datetime", "dateTrunc"},
       {"floor", "sparkFloor"},
       {"months_between", "sparkMonthsBetween"},

       // array functions
       {"array", "array"},
       {"shuffle", "arrayShuffle"},
       {"range", "range"}, /// dummy mapping
        {"flatten", "sparkArrayFlatten"},

       // map functions
       {"map", "map"},
       {"get_map_value", "arrayElement"},
       {"map_keys", "mapKeys"},
       {"map_values", "mapValues"},
       {"map_from_arrays", "mapFromArrays"},

       // tuple functions
       {"get_struct_field", "sparkTupleElement"},
       {"get_array_struct_fields", "sparkTupleElement"},
       {"named_struct", "tuple"},

       // table-valued generator function
       {"explode", "arrayJoin"},
       {"posexplode", "arrayJoin"},

       // json functions
       {"flattenJSONStringOnRequired", "flattenJSONStringOnRequired"},
       {"get_json_object", "get_json_object"},
       {"to_json", "toJSONString"},
       {"from_json", "JSONExtract"},
       {"json_tuple", "json_tuple"},
       {"json_array_length", "JSONArrayLength"},
       {"make_decimal", "makeDecimalSpark"},
       {"unscaled_value", "unscaleValueSpark"},

       // runtime filter
       {"might_contain", "bloomFilterContains"}};

static const std::set<std::string> FUNCTION_NEED_KEEP_ARGUMENTS = {"alias"};

DataTypePtr wrapNullableType(substrait::Type_Nullability nullable, DataTypePtr nested_type);
DataTypePtr wrapNullableType(bool nullable, DataTypePtr nested_type);

std::string join(const ActionsDAG::NodeRawConstPtrs & v, char c);

class SerializedPlanParser;
class LocalExecutor;

// Give a condition expression `cond_rel_`, found all columns with nullability that must not containt
// null after this filter.
// It's used to remove nullability of the columns for performance reason.
class NonNullableColumnsResolver
{
public:
    explicit NonNullableColumnsResolver(const DB::Block & header_, SerializedPlanParser & parser_, const substrait::Expression & cond_rel_);
    ~NonNullableColumnsResolver() = default;

    // return column names
    std::set<String> resolve();

private:
    DB::Block header;
    SerializedPlanParser & parser;
    const substrait::Expression & cond_rel;

    std::set<String> collected_columns;

    void visit(const substrait::Expression & expr);
    void visitNonNullable(const substrait::Expression & expr);

    String safeGetFunctionName(const String & function_signature, const substrait::Expression_ScalarFunction & function) const;
};

class SerializedPlanParser
{
private:
    friend class RelParser;
    friend class RelRewriter;
    friend class ASTParser;
    friend class FunctionParser;
    friend class AggregateFunctionParser;
    friend class FunctionExecutor;
    friend class NonNullableColumnsResolver;
    friend class JoinRelParser;
    friend class MergeTreeRelParser;
    friend class ProjectRelParser;

    std::unique_ptr<LocalExecutor> createExecutor(DB::QueryPlanPtr query_plan);

    DB::QueryPlanPtr parse(const std::string_view & plan);
    DB::QueryPlanPtr parse(const substrait::Plan & plan);

public:
    explicit SerializedPlanParser(const ContextPtr & context);

    /// UT only
    DB::QueryPlanPtr parseJson(const std::string_view & json_plan);
    std::unique_ptr<LocalExecutor> createExecutor(const substrait::Plan & plan) { return createExecutor(parse((plan))); }
    ///

    template <bool JsonPlan>
    std::unique_ptr<LocalExecutor> createExecutor(const std::string_view & plan);

    DB::QueryPlanStepPtr parseReadRealWithLocalFile(const substrait::ReadRel & rel);
    DB::QueryPlanStepPtr parseReadRealWithJavaIter(const substrait::ReadRel & rel);

    static bool isReadRelFromJava(const substrait::ReadRel & rel);
    static bool isReadFromMergeTree(const substrait::ReadRel & rel);

    static substrait::ReadRel::LocalFiles parseLocalFiles(const std::string & split_info);
    static substrait::ReadRel::ExtensionTable parseExtensionTable(const std::string & split_info);

    void addInputIter(jobject iter, bool materialize_input)
    {
        input_iters.emplace_back(iter);
        materialize_inputs.emplace_back(materialize_input);
    }

    void addSplitInfo(std::string && split_info) { split_infos.emplace_back(std::move(split_info)); }

    int nextSplitInfoIndex()
    {
        if (split_info_index >= split_infos.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "split info index out of range, split_info_index: {}, split_infos.size(): {}",
                split_info_index,
                split_infos.size());
        return split_info_index++;
    }

    void parseExtensions(const ::google::protobuf::RepeatedPtrField<substrait::extensions::SimpleExtensionDeclaration> & extensions);
    std::shared_ptr<DB::ActionsDAG> expressionsToActionsDAG(
        const std::vector<substrait::Expression> & expressions, const DB::Block & header, const DB::Block & read_schema);
    RelMetricPtr getMetric() { return metrics.empty() ? nullptr : metrics.at(0); }
    const std::unordered_map<std::string, std::string> & getFunctionMapping() { return function_mapping; }

    static std::string getFunctionName(const std::string & function_sig, const substrait::Expression_ScalarFunction & function);

    IQueryPlanStep * addRemoveNullableStep(QueryPlan & plan, const std::set<String> & columns);
    IQueryPlanStep * addRollbackFilterHeaderStep(QueryPlanPtr & query_plan, const Block & input_header);

    static ContextMutablePtr global_context;
    static Context::ConfigurationPtr config;
    static SharedContextHolder shared_context;
    std::vector<QueryPlanPtr> extra_plan_holder;

private:
    static DB::NamesAndTypesList blockToNameAndTypeList(const DB::Block & header);
    DB::QueryPlanPtr parseOp(const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack);
    void
    collectJoinKeys(const substrait::Expression & condition, std::vector<std::pair<int32_t, int32_t>> & join_keys, int32_t right_key_start);

    DB::ActionsDAGPtr parseFunction(
        const Block & header,
        const substrait::Expression & rel,
        std::string & result_name,
        DB::ActionsDAGPtr actions_dag = nullptr,
        bool keep_result = false);
    DB::ActionsDAGPtr parseFunctionOrExpression(
        const Block & header,
        const substrait::Expression & rel,
        std::string & result_name,
        DB::ActionsDAGPtr actions_dag = nullptr,
        bool keep_result = false);
    DB::ActionsDAGPtr parseArrayJoin(
        const Block & input,
        const substrait::Expression & rel,
        std::vector<String> & result_names,
        DB::ActionsDAGPtr actions_dag = nullptr,
        bool keep_result = false,
        bool position = false);
    DB::ActionsDAGPtr parseJsonTuple(
        const Block & input,
        const substrait::Expression & rel,
        std::vector<String> & result_names,
        DB::ActionsDAGPtr actions_dag = nullptr,
        bool keep_result = false,
        bool position = false);
    const ActionsDAG::Node * parseFunctionWithDAG(
        const substrait::Expression & rel, std::string & result_name, DB::ActionsDAGPtr actions_dag = nullptr, bool keep_result = false);
    ActionsDAG::NodeRawConstPtrs parseArrayJoinWithDAG(
        const substrait::Expression & rel,
        std::vector<String> & result_name,
        DB::ActionsDAGPtr actions_dag = nullptr,
        bool keep_result = false,
        bool position = false);
    void parseFunctionArguments(
        DB::ActionsDAGPtr & actions_dag,
        ActionsDAG::NodeRawConstPtrs & parsed_args,
        std::string & function_name,
        const substrait::Expression_ScalarFunction & scalar_function);
    void parseFunctionArgument(
        DB::ActionsDAGPtr & actions_dag,
        ActionsDAG::NodeRawConstPtrs & parsed_args,
        const std::string & function_name,
        const substrait::FunctionArgument & arg);
    const DB::ActionsDAG::Node *
    parseFunctionArgument(DB::ActionsDAGPtr & actions_dag, const std::string & function_name, const substrait::FunctionArgument & arg);

    void parseArrayJoinArguments(
        DB::ActionsDAGPtr & actions_dag,
        const std::string & function_name,
        const substrait::Expression_ScalarFunction & scalar_function,
        bool position,
        ActionsDAG::NodeRawConstPtrs & parsed_args,
        bool & is_map);


    const DB::ActionsDAG::Node * parseExpression(DB::ActionsDAGPtr actions_dag, const substrait::Expression & rel);
    const ActionsDAG::Node *
    toFunctionNode(ActionsDAGPtr actions_dag, const String & function, const DB::ActionsDAG::NodeRawConstPtrs & args);
    // remove nullable after isNotNull
    void removeNullableForRequiredColumns(const std::set<String> & require_columns, const ActionsDAGPtr & actions_dag) const;
    std::string getUniqueName(const std::string & name) { return name + "_" + std::to_string(name_no++); }
    static std::pair<DataTypePtr, Field> parseLiteral(const substrait::Expression_Literal & literal);
    void wrapNullable(
        const std::vector<String> & columns, ActionsDAGPtr actions_dag, std::map<std::string, std::string> & nullable_measure_names);
    static std::pair<DB::DataTypePtr, DB::Field> convertStructFieldType(const DB::DataTypePtr & type, const DB::Field & field);

    bool isFunction(substrait::Expression_ScalarFunction rel, String function_name);

    int name_no = 0;
    std::unordered_map<std::string, std::string> function_mapping;
    std::vector<jobject> input_iters;
    std::vector<std::string> split_infos;
    int split_info_index = 0;
    std::vector<bool> materialize_inputs;
    ContextPtr context;
    // for parse rel node, collect steps from a rel node
    std::vector<IQueryPlanStep *> temp_step_collection;
    std::vector<RelMetricPtr> metrics;

public:
    const ActionsDAG::Node * addColumn(DB::ActionsDAGPtr actions_dag, const DataTypePtr & type, const Field & field);
};

template <bool JsonPlan>
std::unique_ptr<LocalExecutor> SerializedPlanParser::createExecutor(const std::string_view & plan)
{
    return createExecutor(JsonPlan ? parseJson(plan) : parse(plan));
}

struct SparkBuffer
{
    char * address;
    size_t size;
};

class LocalExecutor : public BlockIterator
{
public:
    LocalExecutor(const ContextPtr & context_, QueryPlanPtr query_plan, QueryPipeline && pipeline, const Block & header_);
    ~LocalExecutor();

    SparkRowInfoPtr next();
    Block * nextColumnar();
    bool hasNext();

    /// Stop execution, used when task receives shutdown command or executor receives SIGTERM signal
    void cancel();

    Block & getHeader();
    RelMetricPtr getMetric() const { return metric; }
    void setMetric(RelMetricPtr metric_) { metric = metric_; }
    void setExtraPlanHolder(std::vector<QueryPlanPtr> & extra_plan_holder_) { extra_plan_holder = std::move(extra_plan_holder_); }

    static void cancelAll();
    static void addExecutor(LocalExecutor * executor);
    static void removeExecutor(Int64 handle);

private:
    std::unique_ptr<SparkRowInfo> writeBlockToSparkRow(const DB::Block & block) const;

    void asyncCancel();
    void waitCancelFinished();

    /// Dump processor runtime information to log
    std::string dumpPipeline() const;

    QueryPipeline query_pipeline;
    std::unique_ptr<PullingPipelineExecutor> executor;
    Block header;
    ContextPtr context;
    std::unique_ptr<CHColumnToSparkRow> ch_column_to_spark_row;
    std::unique_ptr<SparkBuffer> spark_buffer;
    QueryPlanPtr current_query_plan;
    RelMetricPtr metric;
    std::vector<QueryPlanPtr> extra_plan_holder;
    std::atomic<bool> is_cancelled{false};

    /// Record all active LocalExecutor in current executor to cancel them when executor receives shutdown command from driver.
    static std::unordered_map<Int64, LocalExecutor *> executors;
    static std::mutex executors_mutex;
};


class ASTParser
{
public:
    explicit ASTParser(
        const ContextPtr & context_, std::unordered_map<std::string, std::string> & function_mapping_, SerializedPlanParser * plan_parser_)
        : context(context_), function_mapping(function_mapping_), plan_parser(plan_parser_)
    {
    }

    ~ASTParser() = default;

    ASTPtr parseToAST(const Names & names, const substrait::Expression & rel);
    ActionsDAG convertToActions(const NamesAndTypesList & name_and_types, const ASTPtr & ast) const;

private:
    ContextPtr context;
    std::unordered_map<std::string, std::string> function_mapping;
    SerializedPlanParser * plan_parser;

    void parseFunctionArgumentsToAST(const Names & names, const substrait::Expression_ScalarFunction & scalar_function, ASTs & ast_args);
    ASTPtr parseArgumentToAST(const Names & names, const substrait::Expression & rel);
};
}
