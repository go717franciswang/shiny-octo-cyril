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
        while ($row = fgetcsv($this->fh)) {
            $row_out = array();
            foreach ($this->select as $field) {
                if (empty($field->column_mapper)) {
                    $mapped_val = $this->get_column($field->alias, $row);
                } else {
                    $mapped_val = $this->map_columns($field->column_mapper, $row);
                }
                $row_out[] = $mapped_val;
            }
            $result[] = $row_out;
        }
        return $result;
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

    private function map_columns($column_mapper, $row)
    {
        $func = $column_mapper[0];
        $args = array_slice($column_mapper, 1);

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
