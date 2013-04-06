<?php

require_once dirname(__FILE__) . '/csv_field.php';
require_once dirname(__FILE__) . '/csv_exceptions.php';
require_once dirname(__FILE__) . '/csv_column_mappers.php';

class CsvQuery
{
    public $select, $from, $where, $group_by;
    public $headers, $header_idx, $header_count;
    public $return_type = 'list';
    public $delimiter = ',';

    public static function Field($alias, $options = array())
    {
        return new CsvField($alias, $options);
    }

    public function set_headers($headers)
    {
        $this->headers = $headers;
    }

    public function set_delimiter($delimiter)
    {
        $this->delimiter = $delimiter;
    }

    public function execute($options=array())
    {
        if (isset($options['return_type'])) {
            $this->return_type = $options['return_type'];
        }

        $this->validate_statement();
        $this->open_file();
        $this->read_headers();

        $result = array();
        $group_by_cache = array();
        $group_by_cache_idx = array();
        $idx = 0;

        while ($row = $this->get_row()) {
            if (!$this->validate_row($row)) {
                continue;
            }
            $cache = array();

            if ($this->where_exists()) {
                if (!$this->eval_condition($this->where, $row, $cache)) {
                    continue;
                }
            }

            if ($this->group_by_exists()) {
                $cache_ref = &$group_by_cache_idx;
                foreach ($this->group_by as $field) {
                    $key = $this->get_column_mapper($field->final_mapper_key, $field, $row, $cache);
                    if (!isset($cache_ref[$key])) {
                        $cache_ref[$key] = array();
                    }
                    $cache_ref = &$cache_ref[$key];
                }

                if ($cache_ref === array()) {
                    $cache_ref = $idx;
                    $group_by_cache[$cache_ref]['result_row'] = array();
                    $group_by_cache[$cache_ref]['reducers'] = array();
                    $idx += 1;
                }

                foreach ($this->select as $field_idx => $field) {
                    if (empty($field->reducers)) {
                        if (empty($group_by_cache[$cache_ref]['result_row'][$field_idx])) {
                            $group_by_cache[$cache_ref]['result_row'][$field_idx] = 
                                $this->get_column_mapper($field->final_mapper_key, $field, $row, $cache);
                        }
                    } else {
                        foreach ($field->reducers as $reducer_key => $reducer) {
                            $func = $reducer[0];
                            $arg = $reducer[1];
                            if (is_array($arg)) {
                                $key = $arg[1];
                                $val = $this->get_column_mapper($key, $field, $row, $cache);
                            } else {
                                $val = $this->get_column($arg, $row);
                            }

                            if ($func == 'SUM') {
                                if (isset($group_by_cache[$cache_ref]['reducers'][$reducer_key])) {
                                    $group_by_cache[$cache_ref]['reducers'][$reducer_key] += $val;
                                } else {
                                    $group_by_cache[$cache_ref]['reducers'][$reducer_key] = $val;
                                }
                            } elseif ($func == 'MAX') {
                                if (!isset($group_by_cache[$cache_ref]['reducers'][$reducer_key]) ||
                                    $group_by_cache[$cache_ref]['reducers'][$reducer_key] < $val
                                ) {
                                    $group_by_cache[$cache_ref]['reducers'][$reducer_key] = $val;
                                }
                            } elseif ($func == 'MIN') {
                                if (!isset($group_by_cache[$cache_ref]['reducers'][$reducer_key]) ||
                                    $group_by_cache[$cache_ref]['reducers'][$reducer_key] > $val
                                ) {
                                    $group_by_cache[$cache_ref]['reducers'][$reducer_key] = $val;
                                }
                            }
                        }
                    }
                }
                # this unset is extremely important!
                unset($cache_ref);
            } else {
                $row_out = array();
                foreach ($this->select as $field) {
                    $row_out[] = $this->get_column_mapper($field->final_mapper_key, $field, $row, $cache);
                }
                $result[] = $this->build_row($row_out);
            }
        }

        if ($this->group_by_exists()) {
            $levels = count($this->group_by);
            foreach ($group_by_cache as $cache_ref) {
                $cache = $cache_ref['reducers'];
                $row_out = array();
                foreach ($this->select as $field_idx => $field) {
                    if (isset($cache_ref['result_row'][$field_idx])) {
                        $row_out[] = $cache_ref['result_row'][$field_idx];
                    } else {
                        $row_out[] = $this->get_column_mapper(
                            $field->final_mapper_key, $field, array(), $cache
                        );
                    }
                }
                $result[] = $this->build_row($row_out);
            }
        }

        fclose($this->fh);
        return $result;
    }

    private function validate_row($row)
    {
        if (count($row) < $this->header_count) {
            return false;
        }
        return true;
    }

    private function build_row($row)
    {
        if ($this->return_type == 'list') {
            return $row;
        } elseif ($this->return_type == 'associative') {
            $new_row = array();
            foreach ($this->select as $idx => $field) {
                $new_row[$field->alias] = $row[$idx];
            }

            return $new_row;
        } else {
            throw new InvalidReturnType($this->return_type);
        }
    }

    private function get_row()
    {
        if ($line = stream_get_line($this->fh, 10000, "\n")) {
            return explode($this->delimiter, $line);
        }
        return null;
    }

    private function get_column_mapper($key, $field, $row, &$cache)
    {
        if (empty($key)) {
            return $this->get_column($field->alias, $row);
        }

        if (!isset($cache[$key])) {
            $column_mapper = $field->column_mappers[$key];
            $func = $column_mapper[0];
            $args = array_slice($column_mapper, 1);
            $args_final = array();
            foreach ($args as $arg) {
                if (is_array($arg)) {
                    $args_final[] = $this->get_column_mapper($arg[1], $field, $row, $cache);
                } else {
                    $args_final[] = $this->get_column($arg, $row);
                }
            }
            $cache[$key] = call_user_func_array($func, $args_final);
        }
        return $cache[$key];
    }

    private function eval_condition($condition, $row, &$cache)
    {
        $func = $condition[0];
        $args = array_slice($condition, 1);
        if ($func == '=') {
            $func = 'CsvColumnMappers::equal';
        } elseif ($func == '!=') {
            $func = 'CsvColumnMappers::not_equal';
        } elseif ($func == 'IN') {
            $func = 'CsvColumnMappers::in';
        } elseif ($func == 'AND') {
            $func = 'CsvColumnMappers::and_operator';
        } elseif ($func == 'OR') {
            $func = 'CsvColumnMappers::or_operator';
        } elseif ($func == '>') {
            $func = 'CsvColumnMappers::gt';
        } elseif ($func == '>=') {
            $func = 'CsvColumnMappers::gte';
        } elseif ($func == '<') {
            $func = 'CsvColumnMappers::lt';
        } elseif ($func == '<=') {
            $func = 'CsvColumnMappers::lte';
        }

        $args_final = array();
        foreach ($args as $arg_idx => $arg) {
            if (is_a($arg, 'CsvField')) {
                $args_final[] = $this->get_column_mapper($arg->final_mapper_key, $arg, $row, $cache);
            } elseif (is_array($arg) && !($func == 'CsvColumnMappers::in' && $arg_idx != 2)) {
                $args_final[] = $this->eval_condition($arg, $row, $cache);
            } else {
                $args_final[] = $arg;
            }
        }

        return call_user_func_array($func, $args_final);
    }

    private function where_exists()
    {
        return !empty($this->where);
    }

    private function group_by_exists()
    {
        return !empty($this->group_by);
    }

    private function validate_statement()
    {
        if (empty($this->select)) {
            throw new IncompleteStatement('Select is missing');
        }

        if (empty($this->from)) {
            throw new IncompleteStatement('From is missing');
        }
    }

    private function open_file()
    {
        $this->fh = fopen($this->from, 'r');
    }

    private function read_headers()
    {
        if (empty($this->headers)) {
            $this->headers = $this->get_row();
        }
        $this->header_idx = array();
        foreach ($this->headers as $idx => $header) {
            $this->header_idx[$header] = $idx;
        }
        $this->header_count = count($this->headers);
    }

    private function get_column($header, $row)
    {
        return $row[$this->header_idx[$header]];
    }

    public function select($fields)
    {
        $this->select = $fields;
        return $this;
    }

    public function from($filepath)
    {
        $this->from = $filepath;
        return $this;
    }

    public function where($conditions)
    {
        $this->where = $conditions;
        return $this;
    }

    public function group_by($fields)
    {
        $this->group_by = $fields;
        return $this;
    }
}
