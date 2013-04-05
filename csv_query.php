<?php

require dirname(__FILE__) . '/csv_field.php';
require dirname(__FILE__) . '/csv_exceptions.php';
require dirname(__FILE__) . '/csv_column_mappers.php';

class CsvQuery
{
    public $select, $from, $where, $group_by;
    public $headers, $header_idx;

    public static function Field($alias, $options = array())
    {
        return new CsvField($alias, $options);
    }

    public function execute()
    {
        $this->validate_statement();
        $this->open_file();
        $this->read_headers();

        $result = array();
        $group_by_cache = array();

        while ($row = fgetcsv($this->fh)) {
            $cache = array();

            if ($this->where_exists()) {
                if (!$this->eval_condition($this->where, $row, $cache)) {
                    continue;
                }
            }

            if ($this->group_by_exists()) {
                $cache_ref = &$group_by_cache;
                foreach ($this->group_by as $field) {
                    $key = $this->get_column_mapper($field->final_mapper_key, $field, $row, $cache);
                    if (!isset($cache_ref[$key])) {
                        $cache_ref[$key] = array();
                    }
                    $cache_ref = &$cache_ref[$key];
                }

                if (!isset($cache_ref['result_row'])) {
                    $cache_ref['result_row'] = array();
                    $cache_ref['reducers'] = array();
                }

                foreach ($this->select as $field_idx => $field) {
                    if (empty($field->reducers)) {
                        if (empty($cache_ref['result_row'][$field_idx])) {
                            $cache_ref['result_row'][$field_idx] = 
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
                                if (isset($cache_ref['reducers'][$reducer_key])) {
                                    $cache_ref['reducers'][$reducer_key] += $val;
                                } else {
                                    $cache_ref['reducers'][$reducer_key] = $val;
                                }
                            } elseif ($func == 'MAX') {
                                if (!isset($cache_ref['reducers'][$reducer_key]) ||
                                    $cache_ref['reducers'][$reducer_key] < $val
                                ) {
                                    $cache_ref['reducers'][$reducer_key] = $val;
                                }
                            } elseif ($func == 'MIN') {
                                if (!isset($cache_ref['reducers'][$reducer_key]) ||
                                    $cache_ref['reducers'][$reducer_key] > $val
                                ) {
                                    $cache_ref['reducers'][$reducer_key] = $val;
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
                $result[] = $row_out;
            }
        }

        if ($this->group_by_exists()) {
            $levels = count($this->group_by);
            while ($cache_ref = self::iterate_cache($group_by_cache, $levels)) {
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
                $result[] = $row_out;
            }
        }

        fclose($this->fh);
        return $result;
    }

    public static function iterate_cache(&$group_by_cache, $level, &$parent=null)
    {
        if (is_null($group_by_cache)) {
            return null;
        }

        if ($level == 1) {
            $val = current($group_by_cache);
            next($group_by_cache);
        } else {
            $val = self::iterate_cache($group_by_cache[key($group_by_cache)], $level - 1, $group_by_cache);
        }
        if (is_null(key($group_by_cache))) {
            if ($parent) {
                next($parent);
            }
        }
        return $val;
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

    private function calculate_value($field, $row)
    {
        if (empty($field->final_mapper)) {
            return $this->get_column($field->alias, $row);
        } else {
            return $this->map_columns($field->final_mapper, $row);
        }
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
        $this->headers = fgetcsv($this->fh);
        $this->header_idx = array();
        foreach ($this->headers as $idx => $header) {
            $this->header_idx[$header] = $idx;
        }
    }

    private function get_column($header, $row)
    {
        return $row[$this->header_idx[$header]];
    }

    private function map_columns($column_mapper, $row, $field)
    {
        $func = $column_mapper[0];
        $args = array_slice($column_mapper, 1);

        if ($func == 'COLUMN_MAPPER' || $func = 'REDUCER') {

        }

        foreach ($args as &$arg) {
            if (is_array($arg)) {
                $arg = $this->map_columns($arg, $row);
            } else {
                $arg = $this->get_column($arg, $row);
            }
        }

        return call_user_func_array($func, $args);
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
