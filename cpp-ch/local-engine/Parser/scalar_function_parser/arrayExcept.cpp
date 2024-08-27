#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Parser/FunctionParser.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
};
};

namespace local_engine
{
class FunctionParserArrayExcept : public FunctionParser
{
public:
    FunctionParserArrayExcept(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }
    ~FunctionParserArrayExcept() override = default;

    static constexpr auto name = "array_except";
    String getName() const override { return name; }

    const DB::ActionsDAG::Node *
    parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(DB::ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        const auto * arr1_arg = parsed_args[0];
        const auto * arr2_arg = parsed_args[1];

        // Create lambda function x -> !has(arr2, x)
        ActionsDAG lambda_actions_dag;
        const auto * arr2_in_lambda = &lambda_actions_dag.addInput(arr2_arg->result_name, arr2_arg->result_type);
        const auto & nested_type = assert_cast<DataTypeArray &>(removeNullable(arr1_arg->result_type)).getNestedType();
        const auto * x_in_lambda = &lambda_actions_dag.addInput("x", nested_type);
        const auto * has_in_lambda = toFunctionNode(lambda_actions_dag, "has", {arr2_in_lambda, x_in_lambda});
        const auto * lambda_output = toFunctionNode(lambda_actions_dag, "not", {has_in_lambda});
        lambda_actions_dag.getOutputs().push_back(lambda_output);
        lambda_actions_dag.removeUnusedActions(Names(1, lambda_output->result_name));

        auto expression_actions_settings = DB::ExpressionActionsSettings::fromContext(getContext(), DB::CompileExpressions::yes);
        auto lambda_actions = std::make_shared<DB::ExpressionActions>(std::move(lambda_actions_dag), expression_actions_settings);

        DB::Names captured_column_names{arr2_in_lambda->result_name};
        NamesAndTypesList lambda_arguments_names_and_types;
        lambda_arguments_names_and_types.emplace_back(x_in_lambda->result_name, x_in_lambda->result_type);
        DB::Names required_column_names = lambda_actions->getRequiredColumns();
        auto function_capture = std::make_shared<FunctionCaptureOverloadResolver>(
            lambda_actions,
            captured_column_names,
            lambda_arguments_names_and_types,
            lambda_output->result_type,
            lambda_output->result_name);
        const auto * lambda_function = &actions_dag.addFunction(function_capture, {arr2_arg}, lambda_output->result_name);

        // Apply arrayFilter with the lambda function
        const auto * array_filter_node = toFunctionNode(actions_dag, "arrayFilter", {lambda_function, arr1_arg});

        // Apply arrayDistinct to the result of arrayFilter
        const auto * array_distinct_node = toFunctionNode(actions_dag, "arrayDistinct", {array_filter_node});
        return convertNodeTypeIfNeeded(substrait_func, array_distinct_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserArrayExcept> register_array_except;
}
