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
#include <Rewriter/RelRewriter.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>
namespace local_engine
{

/// need to avoid conflict with spark builtin functions
enum SelfDefinedFunctionReference
{
    GET_JSON_OBJECT = 1000000,
};

/// Collect all get_json_object functions and group by json strings.
/// Rewrite the get_json_object functions into flattenJSONStringOnRequired + sparktupleElement. This
/// could avoid repeated parsing the same json string and save a lot of time.
class GetJsonObjectFunctionWriter : public RelRewriter
{
public:
    GetJsonObjectFunctionWriter(ParserContextPtr parser_context_) : RelRewriter(parser_context_) { }
    ~GetJsonObjectFunctionWriter() override = default;

    void rewrite(substrait::Rel & rel) override
    {
        if (!rel.has_filter() && !rel.has_project())
        {
            return;
        }
        prepare(rel);
        rewriteImpl(rel);
    }

private:
    std::unordered_map<String, std::set<String>> json_required_fields;

    /// Collect all get_json_object functions and group by json strings
    void prepare(const substrait::Rel & rel)
    {
        if (rel.has_filter())
        {
            auto & expr = rel.filter().condition();
            prepareOnExpression(expr);
        }
        if (rel.has_project())
        {
            for (auto & expr : rel.project().expressions())
            {
                prepareOnExpression(expr);
            }
        }
    }

    void rewriteImpl(substrait::Rel & rel)
    {
        if (rel.has_filter())
        {
            auto * filter = rel.mutable_filter();
            auto * expression = filter->mutable_condition();
            rewriteExpression(*expression);
        }
        if (rel.has_project())
        {
            auto * project = rel.mutable_project();
            auto * exprssions = project->mutable_expressions();
            for (int i = 0; i < project->expressions_size(); ++i)
            {
                auto * expr = exprssions->Mutable(i);
                rewriteExpression(*expr);
            }
        }
    }
    void prepareOnExpression(const substrait::Expression & expr)
    {
        switch (expr.rex_type_case())
        {
            case substrait::Expression::RexTypeCase::kCast: {
                prepareOnExpression(expr.cast().input());
                break;
            }
            case substrait::Expression::RexTypeCase::kIfThen: {
                const auto & if_then = expr.if_then();
                auto condition_nums = if_then.ifs_size();
                for (int i = 0; i < condition_nums; ++i)
                {
                    prepareOnExpression(if_then.ifs(i).if_());
                    prepareOnExpression(if_then.ifs(i).then());
                }
                prepareOnExpression(if_then.else_());
                break;
            }
            case substrait::Expression::RexTypeCase::kSingularOrList: {
                prepareOnExpression(expr.singular_or_list().value());
                break;
            }
            case substrait::Expression::RexTypeCase::kScalarFunction: {
                const auto & scalar_function_pb = expr.scalar_function();
                auto function_signature_name_opt = parser_context->getFunctionNameInSignature(scalar_function_pb);
                if (!function_signature_name_opt)
                    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknow scalar function: {}", scalar_function_pb.DebugString());
                auto function_signature_name = *function_signature_name_opt;
                for (const auto & arg : scalar_function_pb.arguments())
                {
                    if (arg.has_value())
                    {
                        prepareOnExpression(arg.value());
                    }
                }
                if (function_signature_name == "get_json_object")
                {
                    auto json_key = scalar_function_pb.arguments(0).DebugString();
                    if (!json_required_fields.count(json_key))
                    {
                        json_required_fields[json_key] = std::set<String>();
                    }
                    auto & required_fields = json_required_fields.at(json_key);
                    auto json_path_pb = scalar_function_pb.arguments(1).value();
                    if (!json_path_pb.has_literal() || !json_path_pb.literal().has_string())
                    {
                        break;
                    }
                    required_fields.emplace(json_path_pb.literal().string());
                }
                break;
            }
            default:
                break;
        }
    }

    void rewriteExpression(substrait::Expression & expr)
    {
        switch (expr.rex_type_case())
        {
            case substrait::Expression::RexTypeCase::kCast: {
                if (expr.cast().has_input())
                    rewriteExpression(*expr.mutable_cast()->mutable_input());
                break;
            }
            case substrait::Expression::RexTypeCase::kIfThen: {
                auto * if_then = expr.mutable_if_then();
                auto condition_nums = if_then->ifs_size();
                auto * ifs = if_then->mutable_ifs();
                for (int i = 0; i < condition_nums; ++i)
                {
                    rewriteExpression(*ifs->Mutable(i)->mutable_if_());
                    rewriteExpression(*ifs->Mutable(i)->mutable_then());
                }
                rewriteExpression(*if_then->mutable_else_());
                break;
            }
            case substrait::Expression::RexTypeCase::kSingularOrList: {
                rewriteExpression(*expr.mutable_singular_or_list()->mutable_value());
                break;
            }
            case substrait::Expression::RexTypeCase::kScalarFunction: {
                auto & scalar_function_pb = *expr.mutable_scalar_function();
                if (scalar_function_pb.arguments().empty())
                    break;
                auto json_key = scalar_function_pb.arguments(0).DebugString();
                auto function_signature_name_opt = parser_context->getFunctionNameInSignature(scalar_function_pb);
                if (!function_signature_name_opt)
                    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknow scalar function: {}", scalar_function_pb.DebugString());
                auto function_signature_name = *function_signature_name_opt;
                for (auto & arg : *scalar_function_pb.mutable_arguments())
                {
                    if (arg.has_value())
                    {
                        rewriteExpression(*arg.mutable_value());
                    }
                }
                if (function_signature_name == "get_json_object")
                {
                    if (!json_required_fields.count(json_key))
                    {
                        /// This is not expected, but it still could work.
                        LOG_ERROR(&Poco::Logger::get("GetJsonObjectFunctionWriter"), "Cannot find json key {}", json_key);
                        break;
                    }
                    auto & required_fields = json_required_fields.at(json_key);
                    if (required_fields.empty())
                    {
                        break;
                    }
                    auto json_path_pb = scalar_function_pb.arguments(1).value();
                    if (!json_path_pb.has_literal() || !json_path_pb.literal().has_string())
                    {
                        break;
                    }
                    String required_fields_str;
                    int i = 0;
                    for (const auto & field : required_fields)
                    {
                        if (i)
                        {
                            required_fields_str += "|";
                        }
                        required_fields_str += field;
                        i += 1;
                    }

                    substrait::Expression_ScalarFunction decoded_json_function;
                    decoded_json_function.set_function_reference(SelfDefinedFunctionReference::GET_JSON_OBJECT);
                    decoded_json_function.mutable_output_type()->CopyFrom(buildReturnType(required_fields));
                    auto * arg0 = decoded_json_function.add_arguments();
                    arg0->CopyFrom(scalar_function_pb.arguments(0));
                    auto * arg1 = decoded_json_function.add_arguments();
                    arg1->mutable_value()->mutable_literal()->set_string(required_fields_str);

                    substrait::Expression new_get_json_object_arg0;
                    new_get_json_object_arg0.mutable_scalar_function()->CopyFrom(decoded_json_function);
                    *scalar_function_pb.mutable_arguments()->Mutable(0)->mutable_value() = new_get_json_object_arg0;
                }
                break;
            }
            default:
                break;
        }
    }

    substrait::Type buildReturnType(const std::set<std::string> & fields)
    {
        substrait::Type_Struct st;
        for (const auto & field : fields)
        {
            st.add_names(field);
            substrait::Type_String str_type;
            str_type.set_nullability(substrait::Type_Nullability::Type_Nullability_NULLABILITY_NULLABLE);
            st.add_types()->mutable_string()->CopyFrom(str_type);
        }
        substrait::Type res;
        res.mutable_struct_()->CopyFrom(st);
        return res;
    }
};

class ExpressionsRewriter
{
public:
    explicit ExpressionsRewriter(ParserContextPtr parser_context_) : parser_context(parser_context_) { }
    void rewrite(substrait::Rel & rel)
    {
        GetJsonObjectFunctionWriter get_json_object_rewriter(parser_context);
        get_json_object_rewriter.rewrite(rel);
    }

private:
    ParserContextPtr parser_context;
};
}
