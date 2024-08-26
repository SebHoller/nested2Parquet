#include <set>
#include <map>
#include <iostream>
#include <iomanip>
#include <fmt/chrono.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <rapidjson/document.h>
#include <rapidjson/filereadstream.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/schema.h>
#include <rapidjson/reader.h>

#include <arrow/io/file.h>

#include <parquet/stream_writer.h>
#include <parquet/types.h>
#include <parquet/schema.h>

#include "cxxopts.hpp"
#include "date.h"

using namespace std;
using namespace date;
using namespace rapidjson;
using parquet::Repetition;
using parquet::Type;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

struct column
{
    vector<uint8_t> bool_values;
    vector<int32_t> int32_values;
    vector<int64_t> int64_values;
    vector<double> double_values;
    vector<string> string_values;
    vector<parquet::ByteArray> byte_array_values;
    vector<parquet::FixedLenByteArray> fixed_len_byte_array;
    vector<int16_t> repetition_levels;
    vector<int16_t> definition_levels;
};

std::shared_ptr<GroupNode> *global_parquet_schema;
map<string, int> *global_leaf_indices;
parquet::RowGroupWriter *global_rg_writer;
std::shared_ptr<parquet::ParquetFileWriter> *global_file_writer;
uint64_t global_row_count;
uint64_t global_total_row_count;
uint64_t global_num_rows_per_row_group;
vector<uint64_t> *global_buffered_values_estimate;
uint64_t global_row_group_size;
int16_t global_required_count;
int16_t global_repeated_count;
int16_t global_new_array_depth;
column (*global_parquet_data)[];
vector<string> *global_current_keys;
vector<int> *global_found_keys;
set<string> *global_defined_keys;
map<string, set<string>> *global_def_keys_per_object;
map<string, set<string>> *global_def_keys_per_object_individual;
bool global_new_object;
bool global_new_array;
bool global_new_key;
bool global_logs = false;
ofstream *logfile;

int rep_level()
{
    if (global_new_key)
    {
        return 0;
    }
    if (global_new_array)
    {
        return global_new_array_depth;
    }
    return global_repeated_count;
}

parquet::schema::NodePtr getFieldFromPath(vector<string> *current_keys)
{
    string key = (*current_keys).at(0);
    int field_index = (*global_parquet_schema)->FieldIndex(key);
    auto field = (*global_parquet_schema)->field(field_index);
    if ((*current_keys).size() > 1)
    {
        for (int i = 1; i < (*current_keys).size(); i++)
        {
            if (field->is_group())
            {
                std::shared_ptr<GroupNode> group_field = std::static_pointer_cast<GroupNode>(field);
                string new_key = (*current_keys).at(i);
                int new_index = group_field->FieldIndex(new_key);
                field = group_field->field(new_index);
            }
            else
            {
                break;
            }
        }
    }
    return field;
}

bool allFieldsInPathRequired(const vector<string> *current_keys)
{
    string key = (*current_keys).at(0);
    int field_index = (*global_parquet_schema)->FieldIndex(key);
    auto field = (*global_parquet_schema)->field(field_index);
    bool all_required = true;
    if ((*current_keys).size() >= 1)
    {
        for (int i = 1; i < (*current_keys).size(); i++)
        {
            if (field->name() != "list" && field->name() != "element")
            {
                all_required = all_required && field->is_required();
            }
            if (field->is_group())
            {
                std::shared_ptr<GroupNode> group_field = std::static_pointer_cast<GroupNode>(field);
                string key = (*current_keys).at(i);
                int new_index = group_field->FieldIndex(key);
                field = group_field->field(new_index);
            }
            else
            {
                // maybe not good, schould fail the parser probably?
                return false;
            }
        }
    }
    return all_required && field->is_required();
}

void buildLeafIndexMap(parquet::schema::NodePtr node, map<string, int> *leaf_indices)
{
    if (node->is_group())
    {
        std::shared_ptr<GroupNode> group_field = std::static_pointer_cast<GroupNode>(node);
        for (int i = 0; i < group_field->field_count(); i++)
        {
            buildLeafIndexMap(group_field->field(i), leaf_indices);
        }
    }
    else
    {
        string leaf_path = node->path()->ToDotString();
        (*leaf_indices)[leaf_path] = (*leaf_indices).size();
    }
}

struct MyHandler : public BaseReaderHandler<UTF8<>, MyHandler>
{
    bool Null()
    {
        string col = boost::algorithm::join((*global_current_keys), ".");
        int column_index = -1;
        if (global_leaf_indices->find(col) != global_leaf_indices->end())
        {
            column_index = (*global_leaf_indices)[col];
        }

        if (column_index < 0)
        {
            return false;
        }

        auto field = getFieldFromPath(global_current_keys);
        // check if column is required or has type null
        if (field->is_required() && !field->logical_type()->is_null())
        {
            return false;
        }

        (*global_parquet_data)[column_index].definition_levels.push_back(((*global_current_keys).size() - global_required_count) - 1);
        (*global_parquet_data)[column_index].repetition_levels.push_back(rep_level());

        if ((*global_current_keys).back() == "element")
        {
            global_new_array = false;
        }
        global_new_key = false;
        return true;
    }
    bool Bool(bool b)
    {
        string col = boost::algorithm::join((*global_current_keys), ".");
        int column_index = -1;
        if (global_leaf_indices->find(col) != global_leaf_indices->end())
        {
            column_index = (*global_leaf_indices)[col];
        }

        if (column_index < 0)
        {
            return false;
        }

        auto column_type = global_rg_writer->column(column_index)->type();
        if (column_type != parquet::Type::BOOLEAN)
        {
            return false;
        }

        (*global_parquet_data)[column_index].bool_values.push_back(b);
        (*global_parquet_data)[column_index].definition_levels.push_back((*global_current_keys).size() - global_required_count);
        (*global_parquet_data)[column_index].repetition_levels.push_back(rep_level());

        if ((*global_current_keys).back() == "element")
        {
            global_new_array = false;
        }
        global_new_key = false;
        return true;
    }
    bool Int(int i)
    {
        // check column type -> might be small number but Int64
        string col = boost::algorithm::join((*global_current_keys), ".");
        int column_index = -1;
        if (global_leaf_indices->find(col) != global_leaf_indices->end())
        {
            column_index = (*global_leaf_indices)[col];
        }

        if (column_index < 0)
        {
            return false;
        }

        auto field = getFieldFromPath(global_current_keys);
        if (field->logical_type()->is_nested())
        {
            return false;
        }

        auto column_type = global_rg_writer->column(column_index)->type();
        // switch if type is DOUBLE
        if (column_type == parquet::Type::DOUBLE)
        {
            return Double(i);
        }

        if (!field->logical_type()->is_int())
        {
            return false;
        }

        // switch if type is INT64
        if (column_type == parquet::Type::INT64)
        {
            return Int64(i);
        }

        (*global_parquet_data)[column_index].int32_values.push_back(i);
        (*global_parquet_data)[column_index].definition_levels.push_back((*global_current_keys).size() - global_required_count);
        (*global_parquet_data)[column_index].repetition_levels.push_back(rep_level());

        if ((*global_current_keys).back() == "element")
        {
            global_new_array = false;
        }
        global_new_key = false;
        return true;
    }
    bool Uint(unsigned u)
    {
        // be careful with type (IntType(size, bool_signed))
        // check column type -> might be small number but Int64
        string col = boost::algorithm::join((*global_current_keys), ".");
        int column_index = -1;
        if (global_leaf_indices->find(col) != global_leaf_indices->end())
        {
            column_index = (*global_leaf_indices)[col];
        }

        if (column_index < 0)
        {
            return false;
        }

        auto field = getFieldFromPath(global_current_keys);
        if (field->logical_type()->is_nested())
        {
            return false;
        }

        auto column_type = global_rg_writer->column(column_index)->type();
        // switch if type is DOUBLE
        if (column_type == parquet::Type::DOUBLE)
        {
            return Double(u);
        }

        if (!field->logical_type()->is_int())
        {
            return false;
        }

        // switch if type is INT64
        if (column_type == parquet::Type::INT64)
        {
            return Uint64(u);
        }

        (*global_parquet_data)[column_index].int32_values.push_back(u);
        (*global_parquet_data)[column_index].definition_levels.push_back((*global_current_keys).size() - global_required_count);
        (*global_parquet_data)[column_index].repetition_levels.push_back(rep_level());

        if ((*global_current_keys).back() == "element")
        {
            global_new_array = false;
        }
        global_new_key = false;
        return true;
    }
    bool Int64(int64_t i)
    {
        string col = boost::algorithm::join((*global_current_keys), ".");
        int column_index = -1;
        if (global_leaf_indices->find(col) != global_leaf_indices->end())
        {
            column_index = (*global_leaf_indices)[col];
        }

        if (column_index < 0)
        {
            return false;
        }

        auto field = getFieldFromPath(global_current_keys);

        if (field->logical_type()->is_nested())
        {
            return false;
        }

        // switch if type is DOUBLE
        if (global_rg_writer->column(column_index)->type() == parquet::Type::DOUBLE)
        {
            return Double(i);
        }

        if (!field->logical_type()->is_int())
        {
            return false;
        }

        (*global_parquet_data)[column_index].int64_values.push_back(i);
        (*global_parquet_data)[column_index].definition_levels.push_back((*global_current_keys).size() - global_required_count);
        (*global_parquet_data)[column_index].repetition_levels.push_back(rep_level());

        string last_key = (*global_current_keys)[-1];
        if ((*global_current_keys).back() == "element")
        {
            global_new_array = false;
        }
        global_new_key = false;
        return true;
    }
    bool Uint64(uint64_t u)
    {
        string col = boost::algorithm::join((*global_current_keys), ".");
        int column_index = -1;
        if (global_leaf_indices->find(col) != global_leaf_indices->end())
        {
            column_index = (*global_leaf_indices)[col];
        }

        if (column_index < 0)
        {
            return false;
        }

        auto field = getFieldFromPath(global_current_keys);

        if (field->logical_type()->is_nested())
        {
            return false;
        }

        // switch if type is DOUBLE
        if (global_rg_writer->column(column_index)->type() == parquet::Type::DOUBLE)
        {
            return Double(u);
        }

        if (!field->logical_type()->is_int())
        {
            return false;
        }

        (*global_parquet_data)[column_index].int64_values.push_back(u);
        (*global_parquet_data)[column_index].definition_levels.push_back((*global_current_keys).size() - global_required_count);
        (*global_parquet_data)[column_index].repetition_levels.push_back(rep_level());

        if ((*global_current_keys).back() == "element")
        {
            global_new_array = false;
        }
        global_new_key = false;
        return true;
    }
    bool Double(double d)
    {
        string col = boost::algorithm::join((*global_current_keys), ".");
        int column_index = -1;
        if (global_leaf_indices->find(col) != global_leaf_indices->end())
        {
            column_index = (*global_leaf_indices)[col];
        }

        if (column_index < 0)
        {
            return false;
        }

        auto field = getFieldFromPath(global_current_keys);
        if (field->logical_type()->is_nested())
        {
            return false;
        }

        (*global_parquet_data)[column_index].double_values.push_back(d);
        (*global_parquet_data)[column_index].definition_levels.push_back((*global_current_keys).size() - global_required_count);
        (*global_parquet_data)[column_index].repetition_levels.push_back(rep_level());

        if ((*global_current_keys).back() == "element")
        {
            global_new_array = false;
        }
        global_new_key = false;
        return true;
    }
    bool String(const char *str, SizeType length, bool copy)
    {
        // String should also contain/differ between other types and normal string
        // date, transform into INT32
        // timestamp, transform into INT64
        string col = boost::algorithm::join((*global_current_keys), ".");
        int column_index = -1;
        if (global_leaf_indices->find(col) != global_leaf_indices->end())
        {
            column_index = (*global_leaf_indices)[col];
        }

        if (column_index < 0)
        {
            return false;
        }

        auto field = getFieldFromPath(global_current_keys);

        if (field->logical_type()->is_date())
        {
            // transform string into INT32 (num of days from unix epoch, 01.01.1970)
            tm time = {};
            istringstream ss(str);
            ss >> get_time(&time, "%Y-%m-%d");
            if (ss.fail())
            {
                return false;
            }
            time_t date = mktime(&time);
            int32_t daysSinceEpoch = date / (60 * 60 * 24);
            (*global_parquet_data)[column_index].int32_values.push_back(daysSinceEpoch + 1);
        }
        else
        {
            // default
            (*global_parquet_data)[column_index].string_values.push_back(str);
        }

        (*global_parquet_data)[column_index].definition_levels.push_back((*global_current_keys).size() - global_required_count);
        (*global_parquet_data)[column_index].repetition_levels.push_back(rep_level());

        if ((*global_current_keys).back() == "element")
        {
            global_new_array = false;
        }
        global_new_key = false;
        return true;
    }
    bool StartObject()
    {
        global_new_object = true;

        return true;
    }
    bool Key(const char *str, SizeType length, bool copy)
    {
        string prev_path = boost::algorithm::join((*global_current_keys), ".");
        if (global_new_object)
        {
            global_new_object = false;
        }
        else
        {
            auto prev_field = getFieldFromPath(global_current_keys);
            if (prev_field->path()->ToDotString() != prev_path)
            {
                // fail parser if field not found
                return false;
            }

            if (prev_field->is_required())
            {
                global_required_count--;
            }
            (*global_current_keys).pop_back();
        }

        string parent = boost::algorithm::join((*global_current_keys), ".");
        (*global_def_keys_per_object)[parent].emplace(str);
        (*global_def_keys_per_object_individual)[parent].emplace(str);

        (*global_current_keys).push_back(str);
        string col = boost::algorithm::join((*global_current_keys), ".");
        if ((*global_defined_keys).find(col) == (*global_defined_keys).end())
        {
            global_new_key = true;
        }
        (*global_defined_keys).emplace(col);
        int index = -1;
        if (global_leaf_indices->find(col) != global_leaf_indices->end())
        {
            index = (*global_leaf_indices)[col];
        }
        // only add leaf keys
        if (index >= 0)
        {
            // only add key if not already found
            if (std::find((*global_found_keys).begin(), (*global_found_keys).end(), index) == (*global_found_keys).end())
            {
                (*global_found_keys).push_back(index);
            }
        }
        auto col_field = getFieldFromPath(global_current_keys);
        if (col_field->path()->ToDotString() != col)
        {
            // fail parser if field not found
            return false;
        }
        if (col_field->is_required())
        {
            global_required_count++;
        }

        return true;
    }

    bool checkChildren(parquet::schema::NodePtr node, bool req_parent = false)
    {
        string parent_path = node->parent()->path()->ToDotString();
        (*global_defined_keys).emplace(node->path()->ToDotString());
        // still on path and not leaf
        if (node->is_group())
        {
            // parent is required, so all required children need to be defined
            // parent required AND required AND undefined
            if (req_parent &&
                node->is_required() &&
                ((global_def_keys_per_object->find(parent_path) == global_def_keys_per_object->end()) ||
                 (*global_def_keys_per_object)[parent_path].find(node->name()) == (*global_def_keys_per_object)[parent_path].end()))
            {
                return false;
            }
            bool no_error = true;
            bool req_fields_defined = true;
            std::shared_ptr<GroupNode> step_group_field = std::static_pointer_cast<GroupNode>(node);
            for (int i = 0; i < step_group_field->field_count(); i++)
            {
                no_error = no_error && checkChildren(step_group_field->field(i), node->is_required());
                // for each node on path check required
                // if required, then fail parser if any required child is missing
                if (step_group_field->field(i)->is_required())
                {
                    req_fields_defined = req_fields_defined &&
                                         ((global_def_keys_per_object->find(parent_path) != global_def_keys_per_object->end()) &&
                                          (*global_def_keys_per_object)[parent_path].find(step_group_field->field(i)->name()) != (*global_def_keys_per_object)[parent_path].end());
                }
            }
            global_def_keys_per_object->erase(step_group_field->path()->ToDotString());
            if (req_parent && !req_fields_defined)
            {
                return false;
            }
            return no_error;
        }

        // reached leaf -> get full path
        string leaf_path = node->path()->ToDotString();
        int leaf_index = -1;
        if (global_leaf_indices->find(leaf_path) != global_leaf_indices->end())
        {
            leaf_index = (*global_leaf_indices)[leaf_path];
        }
        if (leaf_index < 0)
        {
            // leaf not found in schema -> something is wrong
            return false;
        }

        if (std::find((*global_found_keys).begin(), (*global_found_keys).end(), leaf_index) == (*global_found_keys).end())
        {
            // not found in keys of this row
            // should only be checked if all parents are also required
            if (allFieldsInPathRequired(&(node->path()->ToDotVector())))
            {
                // leaf not found but is required
                return false;
            }
            (*global_found_keys).push_back(leaf_index);
        }

        // get correct node_name, if element take current_keys
        string node_name = node->name();
        if (node_name == "element")
        {
            node_name = node->path()->ToDotVector().at((*global_current_keys).size());
            parent_path = boost::algorithm::join((*global_current_keys), ".");
        }

        // new key in this object
        if ((global_def_keys_per_object->find(parent_path) == global_def_keys_per_object->end()) || ((*global_def_keys_per_object)[parent_path].find(node_name) == (*global_def_keys_per_object)[parent_path].end()))
        {
            global_new_key = true;
            (*global_def_keys_per_object)[parent_path].emplace(node_name);
        }

        (*global_parquet_data)[leaf_index].definition_levels.push_back((*global_current_keys).size() - global_required_count);
        (*global_parquet_data)[leaf_index].repetition_levels.push_back(rep_level());
        global_new_key = false;
        global_new_array = false;

        return true;
    }
    bool EndObject(SizeType memberCount)
    {
        // end of object, so remove key from stack (last key within object, only while nested)
        if (memberCount > 0)
        {
            string prev_path = boost::algorithm::join((*global_current_keys), ".");
            auto prev_field = getFieldFromPath(global_current_keys);
            if (prev_field->path()->ToDotString() != prev_path)
            {
                // fail parser if field not found
                return false;
            }

            if (prev_field->is_required())
            {
                global_required_count--;
            }
            (*global_current_keys).pop_back();
        }

        global_new_object = false;
        global_new_array = false;

        string current_path = boost::algorithm::join((*global_current_keys), ".");

        parquet::schema::NodePtr current_field = (*global_parquet_schema);
        if ((*global_current_keys).size() > 0)
        {
            current_field = getFieldFromPath(global_current_keys);
        }
        if (current_field->path()->ToDotString() != current_path)
        {
            return false;
        }

        if (!(current_field->is_group()))
        {
            // should be group, otherwise no object
            // something is wrong, fail parser
            return false;
        }

        bool root_result = true;

        std::shared_ptr<GroupNode> group_field = std::static_pointer_cast<GroupNode>(current_field);
        // check if fields are missing, otherwise done with object
        if (memberCount != group_field->field_count())
        {
            // get all root keys
            // -> checkChildren for all missing ones
            for (int i = 0; i < group_field->field_count(); i++)
            {
                auto field = group_field->field(i);
                // key is not already defined in object
                if ((global_def_keys_per_object_individual->find(current_path) == global_def_keys_per_object_individual->end()) || ((*global_def_keys_per_object_individual)[current_path].find(field->name()) == (*global_def_keys_per_object_individual)[current_path].end()))
                {
                    // if parent is list, ignore def_keys_per_object
                    if ((*global_current_keys).size() > 0 && (string)group_field->parent()->name() == "list")
                    {
                        root_result = root_result && checkChildren(field);
                    }
                    else if ((global_def_keys_per_object->find(current_path) == global_def_keys_per_object->end()) || ((*global_def_keys_per_object)[current_path].find(field->name()) == (*global_def_keys_per_object)[current_path].end()))
                    {
                        // iterate over children (DFS, children of children)
                        root_result = root_result && checkChildren(field);
                    }
                }
            }
        }

        // json should be array of rows -> each row is one object
        // if EndObject is also end of row:
        if ((*global_current_keys).size() == 0) // only 0 at end of row
        {
            (*global_found_keys).clear();
            (*global_defined_keys).clear();

            // write whole row to current row_group
            try
            {
                uint64_t estimated_bytes = 0;
                int num_columns = (*global_file_writer)->num_columns();
                // Get the estimated size of the values that are not written to a page yet
                for (int n = 0; n < num_columns; n++)
                {
                    estimated_bytes += (*global_buffered_values_estimate)[n];
                }

                // We need to consider the compressed pages
                // as well as the values that are not compressed yet
                uint64_t total_bytes_written = global_rg_writer->total_bytes_written();
                uint64_t total_compressed_bytes = global_rg_writer->total_compressed_bytes();
                if (((total_bytes_written + total_compressed_bytes + estimated_bytes) > global_row_group_size) || global_row_count >= global_num_rows_per_row_group)
                {
                    global_rg_writer->Close();
                    std::fill(global_buffered_values_estimate->begin(), global_buffered_values_estimate->end(), 0);
                    global_rg_writer = (*global_file_writer)->AppendBufferedRowGroup();
                    global_row_count = 0;
                }

                // for column in parquet schema: generate column_writer
                for (int col = 0; col < num_columns; col++)
                {

                    column row_data = (*global_parquet_data)[col];
                    // def and rep level should be the same
                    int data_length = row_data.definition_levels.size();

                    // get type from file/rg writer and switch column_writer accordingly
                    auto column = global_rg_writer->column(col);
                    auto column_type = column->type();
                    auto col_log_type = column->descr()->logical_type();

                    if (column_type == parquet::Type::BOOLEAN)
                    {
                        parquet::BoolWriter *bool_writer = static_cast<parquet::BoolWriter *>(column);
                        bool tmp_values[row_data.bool_values.size()];
                        for (int i = 0; i < row_data.bool_values.size(); i++)
                        {
                            tmp_values[i] = row_data.bool_values[i];
                        }
                        bool_writer->WriteBatch(data_length, &row_data.definition_levels[0], &row_data.repetition_levels[0], &tmp_values[0]);
                        (*global_buffered_values_estimate)[col] = bool_writer->estimated_buffered_value_bytes();
                    }
                    else if (column_type == parquet::Type::INT32)
                    {
                        parquet::Int32Writer *int32_writer = static_cast<parquet::Int32Writer *>(column);
                        int32_writer->WriteBatch(data_length, &row_data.definition_levels[0], &row_data.repetition_levels[0], &row_data.int32_values[0]);
                        (*global_buffered_values_estimate)[col] = int32_writer->estimated_buffered_value_bytes();
                    }
                    else if (column_type == parquet::Type::INT64)
                    {
                        parquet::Int64Writer *int64_writer = static_cast<parquet::Int64Writer *>(column);
                        int64_writer->WriteBatch(data_length, &row_data.definition_levels[0], &row_data.repetition_levels[0], &row_data.int64_values[0]);
                        (*global_buffered_values_estimate)[col] = int64_writer->estimated_buffered_value_bytes();
                    }
                    else if (column_type == parquet::Type::DOUBLE)
                    {
                        parquet::DoubleWriter *double_writer = static_cast<parquet::DoubleWriter *>(column);
                        double_writer->WriteBatch(data_length, &row_data.definition_levels[0], &row_data.repetition_levels[0], &row_data.double_values[0]);
                        (*global_buffered_values_estimate)[col] = double_writer->estimated_buffered_value_bytes();
                    }
                    else if (column_type == parquet::Type::BYTE_ARRAY)
                    {
                        parquet::ByteArrayWriter *byte_array_writer = static_cast<parquet::ByteArrayWriter *>(column);
                        if (col_log_type->is_string())
                        {
                            vector<parquet::ByteArray> tmp_values;
                            for (int i = 0; i < row_data.string_values.size(); i++)
                            {
                                tmp_values.push_back(parquet::ByteArray(row_data.string_values[i]));
                            }
                            byte_array_writer->WriteBatch(data_length, &row_data.definition_levels[0], &row_data.repetition_levels[0], &tmp_values[0]);
                        }
                        else
                        {
                            byte_array_writer->WriteBatch(data_length, &row_data.definition_levels[0], &row_data.repetition_levels[0], &row_data.byte_array_values[0]);
                        }
                        (*global_buffered_values_estimate)[col] = byte_array_writer->estimated_buffered_value_bytes();
                    }
                    else if (column_type == parquet::Type::FIXED_LEN_BYTE_ARRAY)
                    {
                        parquet::FixedLenByteArrayWriter *fixed_len_byte_array_writer = static_cast<parquet::FixedLenByteArrayWriter *>(column);
                        fixed_len_byte_array_writer->WriteBatch(data_length, &row_data.definition_levels[0], &row_data.repetition_levels[0], &row_data.fixed_len_byte_array[0]);
                        (*global_buffered_values_estimate)[col] = fixed_len_byte_array_writer->estimated_buffered_value_bytes();
                    }

                    (*global_parquet_data)[col].definition_levels.clear();
                    (*global_parquet_data)[col].repetition_levels.clear();
                    (*global_parquet_data)[col].bool_values.clear();
                    (*global_parquet_data)[col].int32_values.clear();
                    (*global_parquet_data)[col].int64_values.clear();
                    (*global_parquet_data)[col].double_values.clear();
                    (*global_parquet_data)[col].byte_array_values.clear();
                    (*global_parquet_data)[col].fixed_len_byte_array.clear();
                    (*global_parquet_data)[col].string_values.clear();
                }
                global_row_count++;
                global_total_row_count++;

                if (global_logs)
                {
                    estimated_bytes = 0;
                    // Get the estimated size of the values that are not written to a page yet
                    for (int n = 0; n < num_columns; n++)
                    {
                        estimated_bytes += (*global_buffered_values_estimate)[n];
                    }

                    // We need to consider the compressed pages
                    // as well as the values that are not compressed yet
                    total_bytes_written = global_rg_writer->total_bytes_written();
                    total_compressed_bytes = global_rg_writer->total_compressed_bytes();
                    if (((total_bytes_written + total_compressed_bytes + estimated_bytes) > global_row_group_size) || global_row_count >= global_num_rows_per_row_group)
                    {
                        auto now = std::chrono::system_clock::now();
                        ostringstream oss;
                        oss << now << ": FINISH row group, rows in row group: " << global_row_count << ", total rows written: " << global_total_row_count << "\n";
                        string log = oss.str();
                        fmt::print(log);
                        if (logfile->is_open())
                        {
                            (*logfile) << log;
                        }
                    }
                }
            }
            catch (const std::exception &e)
            {
                auto now = std::chrono::system_clock::now();
                ostringstream oss;
                oss << now << ": Writing error: " << e.what() << "\n";
                string log = oss.str();
                fmt::print(log);
                if (logfile->is_open())
                {
                    (*logfile) << log;
                }
                return false;
            }
        }

        // need to keep entry within array but delete at end of array
        // only delete if current_path is not repeated
        if ((*global_current_keys).size() == 0 || (string)group_field->parent()->name() != "list")
        {
            global_def_keys_per_object->erase(current_path);
        }
        global_def_keys_per_object_individual->erase(current_path);
        return root_result;
    }
    bool StartArray()
    {
        // check if current field is repeated
        if ((*global_current_keys).size() > 0)
        {
            (*global_current_keys).push_back("list");
            auto field = getFieldFromPath(global_current_keys);
            // fail if field is not repeated
            if (!field->is_repeated())
            {
                return false;
            }
            string list_col = boost::algorithm::join((*global_current_keys), ".");
            (*global_current_keys).push_back("element");
            string col = boost::algorithm::join((*global_current_keys), ".");
            // neither list nor element in defined
            if ((*global_defined_keys).find(list_col) == (*global_defined_keys).end() && ((*global_defined_keys).find(col) == (*global_defined_keys).end()))
            {
                global_new_key = true;
            }
            // add [...].list to defined but not element
            (*global_defined_keys).emplace(list_col);

            if (!global_new_array)
            {
                global_new_array_depth = global_repeated_count;
            }
            global_repeated_count++;
            global_new_array = true;
            int index = -1;
            if (global_leaf_indices->find(col) != global_leaf_indices->end())
            {
                index = (*global_leaf_indices)[col];
            }
            // only add leaf keys
            if (index >= 0)
            {
                // only add key if not already found
                if (std::find((*global_found_keys).begin(), (*global_found_keys).end(), index) == (*global_found_keys).end())
                {
                    (*global_found_keys).push_back(index);
                }
            }
        }
        return true;
    }
    bool EndArray(SizeType elementCount)
    {
        bool root_result = true;
        if ((*global_current_keys).size() > 0)
        {
            if (elementCount > 0)
            {
                // there are actual elements in the array
                // add [...].element to defined
                (*global_defined_keys).emplace(boost::algorithm::join((*global_current_keys), "."));
            }
            // save column for empty array column index, but remove then for depth
            string col = boost::algorithm::join((*global_current_keys), ".");
            // remove "element"
            (*global_current_keys).pop_back();
            // remove "list"
            (*global_current_keys).pop_back();
            if (elementCount == 0)
            {
                int column_index = -1;
                if (global_leaf_indices->find(col) != global_leaf_indices->end())
                {
                    column_index = (*global_leaf_indices)[col];
                }
                if (column_index >= 0)
                {
                    (*global_parquet_data)[column_index].definition_levels.push_back(((*global_current_keys).size() - global_required_count));
                    (*global_parquet_data)[column_index].repetition_levels.push_back(rep_level());
                    global_new_key = false;
                    global_new_array = false;
                }
                else
                {
                    // array is not leaf -> add def, rep for all leaves
                    // get current field
                    parquet::schema::NodePtr current_field = (*global_parquet_schema);
                    if ((*global_current_keys).size() > 0)
                    {
                        for (int i = 0; i < (*global_current_keys).size(); i++)
                        {
                            if (current_field->is_group())
                            {
                                std::shared_ptr<GroupNode> group_field = std::static_pointer_cast<GroupNode>(current_field);
                                string key = (*global_current_keys).at(i);
                                int new_index = group_field->FieldIndex(key);
                                current_field = group_field->field(new_index);
                            }
                            else
                            {
                                // something went wrong, path should have group nodes
                                return false;
                            }
                        }
                    }
                    std::shared_ptr<GroupNode> group_field = std::static_pointer_cast<GroupNode>(current_field);
                    // check if fields are missing, otherwise done with object
                    // get all root keys
                    // -> checkChildren for all missing ones
                    for (int i = 0; i < group_field->field_count(); i++)
                    {
                        auto field = group_field->field(i);
                        root_result = root_result && checkChildren(field);
                    }
                }
            }
            global_repeated_count--;
            global_def_keys_per_object->erase(col);
            global_def_keys_per_object_individual->erase(col);
        }
        return root_result;
    }
};

static std::shared_ptr<parquet::schema::Node> createNode(string key, rapidjson::Value::Object *object, bool required)
{
    assert(object->HasMember("type"));
    assert((*object)["type"].IsString());
    string type = (*object)["type"].GetString();
    if (type == "array")
    {
        assert(object->HasMember("items"));
        assert((*object)["items"].IsObject());
        auto items = (*object)["items"].GetObject();
        auto array_element = createNode("element", &items, false);

        parquet::schema::NodeVector list_element;
        list_element.push_back(array_element);
        auto array = GroupNode::Make("list", Repetition::REPEATED, list_element);

        parquet::schema::NodeVector list_elements;
        list_elements.push_back(array);

        if (required)
        {
            return GroupNode::Make(key, Repetition::REQUIRED, list_elements, parquet::LogicalType::List());
        }

        return GroupNode::Make(key, Repetition::OPTIONAL, list_elements, parquet::LogicalType::List());
    }
    else if (type == "object")
    {
        parquet::schema::NodeVector column_object;

        vector<string> required_fields;
        if (object->HasMember("required"))
        {
            assert((*object)["required"].IsArray());
            for (auto &req : (*object)["required"].GetArray())
            {
                required_fields.push_back((string)req.GetString());
            }
        }

        assert(object->HasMember("properties"));
        assert((*object)["properties"].IsObject());
        for (auto &prop : (*object)["properties"].GetObject())
        {
            string member_key = prop.name.GetString();
            rapidjson::Value::Object member_value = prop.value.GetObject();

            if (std::find(required_fields.begin(), required_fields.end(), member_key) != required_fields.end())
            {
                column_object.push_back(createNode(member_key, &member_value, true));
            }
            else
            {
                column_object.push_back(createNode(member_key, &member_value, false));
            }
        }

        if (required)
        {
            return GroupNode::Make(key, Repetition::REQUIRED, column_object);
        }
        return GroupNode::Make(key, Repetition::OPTIONAL, column_object);
    }
    else if (type == "string")
    {
        // check format
        if (object->HasMember("format"))
        {
            assert((*object)["format"].IsString());
            string format = (*object)["format"].GetString();
            if (format == "date")
            {
                if (required)
                {
                    return PrimitiveNode::Make(key, Repetition::REQUIRED, parquet::LogicalType::Date(), parquet::Type::INT32);
                }
                return PrimitiveNode::Make(key, Repetition::OPTIONAL, parquet::LogicalType::Date(), parquet::Type::INT32);
            }
        }
        if (required)
        {
            return PrimitiveNode::Make(key, Repetition::REQUIRED, parquet::LogicalType::String(), parquet::Type::BYTE_ARRAY);
        }
        return PrimitiveNode::Make(key, Repetition::OPTIONAL, parquet::LogicalType::String(), parquet::Type::BYTE_ARRAY);
    }
    else if (type == "integer")
    {
        bool set_min = false;
        int64_t minimum = INT64_MAX;
        bool set_max = false;
        int64_t maximum = INT64_MIN;
        bool set_umax = false;
        uint64_t umaximum = 0;
        if (object->HasMember("minimum"))
        {
            assert((*object)["minimum"].IsInt64());
            set_min = true;
            minimum = (*object)["minimum"].GetInt64();
        }
        if (object->HasMember("maximum"))
        {
            if ((*object)["maximum"].IsInt64())
            {
                set_max = true;
                maximum = (*object)["maximum"].GetInt64();
            }
            else if ((*object)["maximum"].IsUint64())
            {
                set_max = true;
                set_umax = true;
                umaximum = (*object)["maximum"].GetUint64();
            }
        }
        if (object->HasMember("exclusiveMinimum"))
        {
            if ((*object)["exclusiveMinimum"].IsInt64())
            {
                set_min = true;
                minimum = (*object)["exclusiveMinimum"].GetInt64() + 1;
            }
            if ((*object)["exclusiveMinimum"].IsBool())
            {
                set_min = true;
                if ((*object)["exclusiveMinimum"].GetBool())
                {
                    minimum++;
                }
            }
        }
        if (object->HasMember("exclusiveMaximum"))
        {
            if ((*object)["exclusiveMaximum"].IsInt64())
            {
                if ((*object)["exclusiveMaximum"].IsInt64())
                {
                    set_max = true;
                    maximum = (*object)["exclusiveMaximum"].GetInt64() - 1;
                }
                else if ((*object)["exclusiveMaximum"].IsUint64())
                {
                    set_max = true;
                    set_umax = true;
                    umaximum = (*object)["exclusiveMaximum"].GetUint64() - 1;
                }
            }
            if ((*object)["exclusiveMaximum"].IsBool())
            {
                if (set_max && (*object)["exclusiveMaximum"].GetBool())
                {
                    maximum--;
                    umaximum--;
                }
            }
        }

        // decide type based on maximum, exclusiveMaximum, minimum, exclusiveMinimum

        // INT32, INT32_signed, INT64, INT64_signed
        // skip INT8 and INT16 as parquet primitive type is minimum INT32
        if (set_min && set_max)
        {
            if (set_umax)
            {
                if (minimum >= 0 && umaximum <= UINT32_MAX)
                {
                    if (required)
                    {
                        return PrimitiveNode::Make(key, Repetition::REQUIRED, parquet::LogicalType::Int(32, false), parquet::Type::INT32);
                    }
                    return PrimitiveNode::Make(key, Repetition::OPTIONAL, parquet::LogicalType::Int(32, false), parquet::Type::INT32);
                }
                if (minimum >= INT32_MIN && umaximum <= INT32_MAX)
                {
                    if (required)
                    {
                        return PrimitiveNode::Make(key, Repetition::REQUIRED, parquet::LogicalType::Int(32, true), parquet::Type::INT32);
                    }
                    return PrimitiveNode::Make(key, Repetition::OPTIONAL, parquet::LogicalType::Int(32, true), parquet::Type::INT32);
                }
                if (minimum >= 0 && umaximum <= UINT64_MAX)
                {
                    if (required)
                    {
                        return PrimitiveNode::Make(key, Repetition::REQUIRED, parquet::LogicalType::Int(64, false), parquet::Type::INT64);
                    }
                    return PrimitiveNode::Make(key, Repetition::OPTIONAL, parquet::LogicalType::Int(64, false), parquet::Type::INT64);
                }
            }
            else
            {
                if (minimum >= 0 && maximum <= UINT32_MAX)
                {
                    if (required)
                    {
                        return PrimitiveNode::Make(key, Repetition::REQUIRED, parquet::LogicalType::Int(32, false), parquet::Type::INT32);
                    }
                    return PrimitiveNode::Make(key, Repetition::OPTIONAL, parquet::LogicalType::Int(32, false), parquet::Type::INT32);
                }
                if (minimum >= INT32_MIN && maximum <= INT32_MAX)
                {
                    if (required)
                    {
                        return PrimitiveNode::Make(key, Repetition::REQUIRED, parquet::LogicalType::Int(32, true), parquet::Type::INT32);
                    }
                    return PrimitiveNode::Make(key, Repetition::OPTIONAL, parquet::LogicalType::Int(32, true), parquet::Type::INT32);
                }
                if (minimum >= 0 && maximum <= UINT64_MAX)
                {
                    if (required)
                    {
                        return PrimitiveNode::Make(key, Repetition::REQUIRED, parquet::LogicalType::Int(64, false), parquet::Type::INT64);
                    }
                    return PrimitiveNode::Make(key, Repetition::OPTIONAL, parquet::LogicalType::Int(64, false), parquet::Type::INT64);
                }
            }
            // else use default
        }

        // default
        if (required)
        {
            return PrimitiveNode::Make(key, Repetition::REQUIRED, parquet::LogicalType::Int(64, true), parquet::Type::INT64);
        }
        return PrimitiveNode::Make(key, Repetition::OPTIONAL, parquet::LogicalType::Int(64, true), parquet::Type::INT64);
    }
    else if (type == "number")
    {
        if (required)
        {
            return PrimitiveNode::Make(key, Repetition::REQUIRED, parquet::Type::DOUBLE);
        }
        return PrimitiveNode::Make(key, Repetition::OPTIONAL, parquet::Type::DOUBLE);
    }
    else if (type == "boolean")
    {
        if (required)
        {
            return PrimitiveNode::Make(key, Repetition::REQUIRED, parquet::Type::BOOLEAN);
        }
        return PrimitiveNode::Make(key, Repetition::OPTIONAL, parquet::Type::BOOLEAN);
    }
    else if (type == "null")
    {
        if (required)
        {
            return PrimitiveNode::Make(key, Repetition::REQUIRED, parquet::LogicalType::Null(), parquet::Type::UNDEFINED);
        }
        return PrimitiveNode::Make(key, Repetition::OPTIONAL, parquet::LogicalType::Null(), parquet::Type::UNDEFINED);
    }

    throw runtime_error("Unsupported type: " + type);
}

static std::pair<std::shared_ptr<GroupNode>, map<string, int>> SetupParquetSchema(Document *schema_doc)
{
    parquet::schema::NodeVector fields;

    // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
    // https://github.com/apache/arrow/blob/main/cpp/src/parquet/schema.cc

    // expect the schema JSON to start like this:
    // {
    //     "$schema": <any schema url>,
    //     "type": "array",
    //     "items": {
    //         "type": "object",
    //         "required": [<required fields>],
    //         "properties": {
    //             "<key>": {
    //                 "type": <type>
    //             }
    //         }
    //     }
    // }

    assert(schema_doc->IsObject());
    assert(schema_doc->HasMember("type"));
    assert((*schema_doc)["type"].IsString());
    assert((string)(*schema_doc)["type"].GetString() == "array");

    assert(schema_doc->HasMember("items"));
    assert((*schema_doc)["items"].IsObject());
    auto items = (*schema_doc)["items"].GetObject();

    assert(items.HasMember("type"));
    assert(items["type"].IsString());
    assert((string)items["type"].GetString() == "object");

    // iterate over all members of properties, keys are column names, values are types
    // do this recursively for nested types

    vector<string> required_fields;
    if (items.HasMember("required"))
    {
        assert(items["required"].IsArray());
        for (auto &req : items["required"].GetArray())
        {
            required_fields.push_back((string)req.GetString());
        }
    }

    assert(items.HasMember("properties"));
    assert(items["properties"].IsObject());
    for (auto &prop : items["properties"].GetObject())
    {
        string member_key = prop.name.GetString();
        rapidjson::Value::Object member_value = prop.value.GetObject();

        if (std::find(required_fields.begin(), required_fields.end(), member_key) != required_fields.end())
        {
            fields.push_back(createNode(member_key, &member_value, true));
        }
        else
        {
            fields.push_back(createNode(member_key, &member_value, false));
        }
    }

    // Create a GroupNode named 'schema' using the primitive nodes defined above
    // This GroupNode is the root node of the schema tree
    std::shared_ptr<parquet::schema::Node> root = GroupNode::Make("schema", Repetition::REQUIRED, fields);

    std::shared_ptr<GroupNode> group_root = std::static_pointer_cast<GroupNode>(root);

    // go through complete schema and build global_leaf_indices
    map<string, int> leaf_indices;
    buildLeafIndexMap(group_root, &leaf_indices);

    return {group_root, leaf_indices};
}

int parseJSONToParquet(string path, SchemaDocument *json_schema, string parquet_name, std::shared_ptr<parquet::WriterProperties> writer_props, int buffersize = 65536, bool logs = false, bool novalidate = false, bool print_duration = false)
{
    int16_t glob_new_array_depth = 0;
    int16_t glob_required_count = 0;
    int16_t glob_repeated_count = 0;
    global_required_count = glob_required_count;
    global_repeated_count = glob_repeated_count;
    global_new_array_depth = glob_new_array_depth;

    int num_columns = global_leaf_indices->size();
    column parquet_data[num_columns];
    global_parquet_data = &parquet_data;

    vector<string> current_keys;
    global_current_keys = &current_keys;

    vector<int> found_keys;
    global_found_keys = &found_keys;

    set<string> defined_keys;
    global_defined_keys = &defined_keys;

    map<string, set<string>> def_keys_per_object;
    global_def_keys_per_object = &def_keys_per_object;

    map<string, set<string>> def_keys_per_object_individual;
    global_def_keys_per_object_individual = &def_keys_per_object_individual;

    global_new_object = false;
    global_new_array = false;
    global_new_key = false;

    FILE *file = fopen(path.c_str(), "r");

    if (!file)
    {
        auto now = std::chrono::system_clock::now();
        ostringstream oss;
        oss << now << ": CANNOT open file: '" << path << "'" << "\n";
        string log = oss.str();
        fmt::print(log);
        if (logfile->is_open())
        {
            (*logfile) << log;
        }
        return -1;
    }

    char readBuffer[buffersize];
    rapidjson::FileReadStream readStream(file, readBuffer, sizeof(readBuffer));
    MyHandler handler;
    Reader handlerReader;

    // Setup Parquet writer
    // Create a local file output stream instance.
    std::shared_ptr<arrow::io::FileOutputStream> out_file;

    PARQUET_ASSIGN_OR_THROW(out_file, arrow::io::FileOutputStream::Open(parquet_name));

    // Create a ParquetFileWriter instance
    std::shared_ptr<parquet::ParquetFileWriter> file_writer =
        parquet::ParquetFileWriter::Open(out_file, (*global_parquet_schema), writer_props);
    // Append a RowGroup
    parquet::RowGroupWriter *rg_writer = file_writer->AppendBufferedRowGroup();

    global_file_writer = &file_writer;
    global_rg_writer = rg_writer;
    global_row_count = 0;
    global_total_row_count = 0;
    global_logs = logs;

    vector<uint64_t> buffered_values_estimate(num_columns, 0);
    global_buffered_values_estimate = &buffered_values_estimate;

    auto now = std::chrono::system_clock::now();
    auto start = now;
    stringstream oss;
    oss << now << ": START Parsing \"" << path << "\"" << "\n";
    string log = oss.str();
    fmt::print(log);
    if (logfile->is_open())
    {
        (*logfile) << log;
    }
    if (novalidate)
    {
        handlerReader.Parse(readStream, handler);
    }
    else
    {
        GenericSchemaValidator<SchemaDocument, MyHandler> validator((*json_schema), handler);
        // see: https://github.com/pah/rapidjson/blob/master/example/schemavalidator/schemavalidator.cpp
        // https://rapidjson.org/md_doc_schema.html
        if (!handlerReader.Parse(readStream, validator) && handlerReader.GetParseErrorCode() == kParseErrorTermination)
        {
            // Not a valid JSON
            // When reader.GetParseResult().Code() == kParseErrorTermination,
            // it may be terminated by:
            // (1) the validator found that the JSON is invalid according to schema; or
            // (2) the input stream has I/O error.

            // Check the validation result
            if (!validator.IsValid())
            {
                // Input JSON is invalid according to the schema
                StringBuffer sb;
                validator.GetInvalidSchemaPointer().StringifyUriFragment(sb);
                now = std::chrono::system_clock::now();
                oss.str(std::string());
                oss << now << ": Invalid schema: " << sb.GetString() << "\n";
                log = oss.str();
                fmt::print(log);
                if (logfile->is_open())
                {
                    (*logfile) << log;
                }
                oss.str(std::string());
                oss << now << ": Invalid keyword: " << validator.GetInvalidSchemaKeyword() << "\n";
                log = oss.str();
                fmt::print(log);
                if (logfile->is_open())
                {
                    (*logfile) << log;
                }
                sb.Clear();
                validator.GetInvalidDocumentPointer().StringifyUriFragment(sb);
                oss.str(std::string());
                oss << now << ": Invalid document: " << sb.GetString() << "\n";
                log = oss.str();
                fmt::print(log);
                if (logfile->is_open())
                {
                    (*logfile) << log;
                }
            }
        }
    }

    fclose(file);
    file_writer->Close();

    if (handlerReader.HasParseError())
    {
        now = std::chrono::system_clock::now();
        oss.str(std::string());
        oss << now << ": Error at '" << handlerReader.GetErrorOffset() << "': " << GetParseError_En(handlerReader.GetParseErrorCode()) << "\n";
        log = oss.str();
        fmt::print(log);
        if (logfile->is_open())
        {
            (*logfile) << log;
        }
        return -1;
    }
    now = std::chrono::system_clock::now();
    auto finish = now;
    oss.str(std::string());
    oss << now << ": FINISH Parsing" << "\n";
    log = oss.str();
    fmt::print(log);
    if (logfile->is_open())
    {
        (*logfile) << log;
    }
    if (logs || print_duration)
    {
        auto duration = chrono::duration_cast<chrono::milliseconds>(finish - start);
        now = std::chrono::system_clock::now();
        oss.str(std::string());
        oss << now << ": Duration: " << duration << "\n";
        log = oss.str();
        fmt::print(log);
        if (logfile->is_open())
        {
            (*logfile) << log;
        }
    }
    return 0;
}

parquet::Compression::type getCompression(string compression)
{
    parquet::Compression::type comp = parquet::Compression::UNCOMPRESSED;
    if (compression == "brotli" || compression == "BROTLI")
    {
        comp = parquet::Compression::BROTLI;
    }
    else if (compression == "bz2" || compression == "BZ2")
    {
        comp = parquet::Compression::BZ2;
    }
    else if (compression == "gzip" || compression == "GZIP")
    {
        comp = parquet::Compression::GZIP;
    }
    else if (compression == "lz4" || compression == "LZ4")
    {
        comp = parquet::Compression::LZ4;
    }
    else if (compression == "lz4_frame" || compression == "LZ4_FRAME")
    {
        comp = parquet::Compression::LZ4_FRAME;
    }
    else if (compression == "lz4_hadoop" || compression == "LZ4_HADOOP")
    {
        comp = parquet::Compression::LZ4_HADOOP;
    }
    else if (compression == "lz0" || compression == "LZ0")
    {
        comp = parquet::Compression::LZO;
    }
    else if (compression == "snappy" || compression == "SNAPPY")
    {
        comp = parquet::Compression::SNAPPY;
    }
    else if (compression == "zstd" || compression == "ZSTD")
    {
        comp = parquet::Compression::ZSTD;
    }
    else if (compression == "uncompressed" || compression == "UNCOMPRESSED")
    {
        comp = parquet::Compression::UNCOMPRESSED;
    }
    return comp;
}

parquet::Encoding::type getEncoding(string encoding)
{
    parquet::Encoding::type enc = parquet::Encoding::PLAIN;
    if (encoding == "byte_stream_split" || encoding == "BYTE_STREAM_SPLIT")
    {
        enc = parquet::Encoding::BYTE_STREAM_SPLIT;
    }
    else if (encoding == "delta_binary_packed" || encoding == "DELTA_BINARY_PACKED")
    {
        enc = parquet::Encoding::DELTA_BINARY_PACKED;
    }
    else if (encoding == "delta_byte_array" || encoding == "DELTA_BYTE_ARRAY")
    {
        enc = parquet::Encoding::DELTA_BYTE_ARRAY;
    }
    else if (encoding == "delta_length_byte_array" || encoding == "DELTA_LENGTH_BYTE_ARRAY")
    {
        enc = parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY;
    }
    else if (encoding == "plain" || encoding == "PLAIN")
    {
        enc = parquet::Encoding::PLAIN;
    }
    else if (encoding == "rle" || encoding == "RLE")
    {
        enc = parquet::Encoding::RLE;
    }
    else if (encoding == "undefined" || encoding == "UNDEFINED")
    {
        enc = parquet::Encoding::UNDEFINED;
    }
    return enc;
}

int main(int argc, const char *argv[])
{
    vector<string> paths;
    string schema_path = "";
    string parquet_name = "";
    int precision = -1;
    int scale = -1;
    string compression = "";
    string encoding = "";
    bool set_by_user = false;
    bool logs = false;
    bool nodictionary = false;
    bool novalidate = false;
    bool print_duration = false;
    string logs_name = "";
    uint64_t buffersize = 65536;

    // one row will be kept in memory!
    // max number of rows in row group, can be less
    uint64_t NUM_ROWS_PER_ROW_GROUP = 1000000;
    // max bytes in row group, except when one row has more bytes -> this row own row group
    uint64_t ROW_GROUP_SIZE = 1 * 1024 * 1024 * 1024; // 1073741824 -> 1 GB

    cxxopts::Options options("nested2Parquet", "This is a parser for nested JSON to Parquet files");
    options.positional_help("[optional args]")
        .show_positional_help();
    options
        .set_tab_expansion()
        .add_options()("s,schema", "The JSON schema file", cxxopts::value<string>())("o,output", "The output parquet filename, but will be ignored when multiple JSON files are given", cxxopts::value<string>())("b,buffer", "The read buffer size. Default: 65536", cxxopts::value<uint64_t>())("r,rows", "The maximum number of rows per row group. Default: 1000000", cxxopts::value<uint64_t>())("z,size", "The maximum number of bytes per row group, except when one single row is larger. Default: 1073741824 (1GB)", cxxopts::value<uint64_t>())("c,compression", "The compression used for the Parquet file. Default: unkompressed. Options are: brotli, bz2, gzip, lz4, lz4_frame, lz4_hadoop, lz0, snappy, zstd, uncompressed", cxxopts::value<string>())("e,encoding", "The default encoding used for the Parquet file. Default: plain. Options are: byte_stream_split, delta_binary_packed, delta_byte_array, delta_length_byte_array, plain, rle, undefined", cxxopts::value<string>())("d,no-dictionary", "Disable dictionary encoding for the Parquet file.", cxxopts::value<bool>()->default_value("false"))("l,logs", "Add a filename here, this will save all logs into the file", cxxopts::value<string>())("u,debug", "Enable additional log outputs while parsing", cxxopts::value<bool>()->default_value("false"))("v,no-validate", "Parse without validating the JSON against the provided schema.", cxxopts::value<bool>()->default_value("false"))("t,duration", "Print the duration at the end of each parsed file. (Also included in debug logs)", cxxopts::value<bool>()->default_value("false"))("positional", "Put the JSON filename(s) here", cxxopts::value<vector<string>>())("h,help", "Print Help");
    options.parse_positional({"positional"});

    auto result_options = options.parse(argc, argv);

    if (result_options.count("help"))
    {
        cout << options.help({""}) << endl;
        return 0;
    }

    if (result_options.count("schema"))
    {
        schema_path = result_options["schema"].as<string>();
    }
    if (result_options.count("output"))
    {
        parquet_name = result_options["output"].as<string>();
    }
    if (result_options.count("buffer"))
    {
        buffersize = result_options["buffer"].as<uint64_t>();
    }
    if (result_options.count("rows"))
    {
        NUM_ROWS_PER_ROW_GROUP = result_options["rows"].as<uint64_t>();
    }
    if (result_options.count("size"))
    {
        ROW_GROUP_SIZE = result_options["size"].as<uint64_t>();
    }
    if (result_options.count("compression"))
    {
        compression = result_options["compression"].as<string>();
    }
    if (result_options.count("encoding"))
    {
        encoding = result_options["encoding"].as<string>();
    }
    if (result_options.count("logs"))
    {
        logs_name = result_options["logs"].as<string>();
    }
    if (result_options.count("debug"))
    {
        logs = true;
    }
    if (result_options.count("no-validate"))
    {
        novalidate = true;
    }
    if (result_options.count("no-dictionary"))
    {
        nodictionary = true;
    }
    if (result_options.count("duration"))
    {
        print_duration = true;
    }
    if (result_options.count("positional"))
    {
        paths = result_options["positional"].as<vector<string>>();
    }

    schema_path.erase(std::remove(schema_path.begin(), schema_path.end(), '\n'), schema_path.cend());
    boost::algorithm::trim(schema_path);
    if (schema_path.length() > 0)
    {
        int lastdot = schema_path.find_last_of('.');
        string ending = schema_path.substr(lastdot + 1, schema_path.length());
        if (ending != "json")
        {
            fmt::println("{}: Input schema file is not JSON: \"{}\"", std::chrono::system_clock::now(), schema_path);
        }
    }

    // build schema here and use for every file
    FILE *schema_file = fopen(schema_path.c_str(), "r");

    if (!schema_file)
    {
        fmt::println("{}: CANNOT open schema file: '{}'", std::chrono::system_clock::now(), schema_path);
        fclose(schema_file);
        return -1;
    }

    Document schema_doc;
    char schemaReadBuffer[4096];
    rapidjson::FileReadStream schemaReadStream(schema_file, schemaReadBuffer, sizeof(schemaReadBuffer));
    schema_doc.ParseStream(schemaReadStream);
    SchemaDocument json_schema(schema_doc);
    fclose(schema_file);

    // expect json schema to be given
    // generate Schema for parquet
    auto schema_tuple = SetupParquetSchema(&schema_doc);
    global_parquet_schema = &schema_tuple.first;
    global_leaf_indices = &schema_tuple.second;

    // write logs to txt
    ofstream logoutput;
    logfile = &logoutput;
    if (logs_name != "")
    {
        logoutput.open(logs_name, ios::app);
    }

    // Add writer properties
    parquet::WriterProperties::Builder builder;
    parquet::Compression::type comp = getCompression(compression);
    builder.compression(comp);
    auto now = std::chrono::system_clock::now();
    stringstream oss;
    string log;
    if (logs)
    {
        map<int, string> parquet_compression;
        parquet_compression[0] = "UNCOMPRESSED";
        parquet_compression[1] = "SNAPPY";
        parquet_compression[2] = "GZIP";
        parquet_compression[3] = "BROTLI";
        parquet_compression[4] = "ZSTD";
        parquet_compression[5] = "LZ4";
        parquet_compression[6] = "LZ4_FRAME";
        parquet_compression[7] = "LZO";
        parquet_compression[8] = "BZ2";
        parquet_compression[9] = "LZ4_HADOOP";
        oss << now << ": Compression: " << parquet_compression[comp] << "\n";
        log = oss.str();
        if (logoutput.is_open())
        {
            logoutput << log;
        }
        fmt::print(log);
    }

    builder.created_by("nested2Parquet");

    parquet::Encoding::type enc = getEncoding(encoding);
    builder.encoding(enc);
    if (logs)
    {
        now = std::chrono::system_clock::now();
        oss.str(std::string());
        oss << now << ": Encoding: " << parquet::EncodingToString(enc) << "\n";
        log = oss.str();
        if (logoutput.is_open())
        {
            logoutput << log;
        }
        fmt::print(log);
    }

    if (nodictionary)
    {
        builder.disable_dictionary();
    }

    auto writer_props = builder.build();

    global_row_group_size = ROW_GROUP_SIZE;
    global_num_rows_per_row_group = NUM_ROWS_PER_ROW_GROUP;

    int res = 0;

    if (paths.size() == 1 && parquet_name != "")
    {
        string path = paths[0];
        path.erase(std::remove(path.begin(), path.end(), '\n'), path.cend());
        boost::algorithm::trim(path);
        if (path.length() > 0)
        {
            int lastdot = path.find_last_of('.');
            string ending = path.substr(lastdot + 1, path.length());
            if (ending != "json")
            {
                now = std::chrono::system_clock::now();
                oss.str(std::string());
                oss << now << ": Input file is not JSON: \"" << path << "\"\n";
                log = oss.str();
                if (logoutput.is_open())
                {
                    logoutput << log;
                }
                fmt::print(log);
            }
            else
            {
                res = parseJSONToParquet(path, &json_schema, parquet_name, writer_props, buffersize, logs, novalidate, print_duration);
            }
        }
    }
    else
    {
        for (string path : paths)
        {
            path.erase(std::remove(path.begin(), path.end(), '\n'), path.cend());
            boost::algorithm::trim(path);
            if (path.length() > 0)
            {
                int lastdot = path.find_last_of('.');
                string ending = path.substr(lastdot + 1, path.length());
                if (ending != "json")
                {
                    now = std::chrono::system_clock::now();
                    oss.str(std::string());
                    oss << now << ": Input file is not JSON: \"" << path << "\"\n";
                    log = oss.str();
                    if (logoutput.is_open())
                    {
                        logoutput << log;
                    }
                    fmt::print(log);
                    continue;
                }
                // ignore parquet_name for multiple JSON files given!
                string file_name = path.substr(0, path.find_last_of('.'));
                res = parseJSONToParquet(path, &json_schema, file_name + ".parquet", writer_props, buffersize, logs, novalidate, print_duration);
                if (res != 0)
                {
                    now = std::chrono::system_clock::now();
                    oss.str(std::string());
                    oss << now << ": Error while parsing " << path << "\n";
                    log = oss.str();
                    if (logoutput.is_open())
                    {
                        logoutput << log;
                    }
                    fmt::print(log);
                }
                if (logoutput.is_open())
                {
                    logoutput.close();
                    logoutput.open(logs_name, ios::app);
                }
            }
        }
    }
    if (logoutput.is_open())
    {
        logoutput.close();
    }
    return res;
}
