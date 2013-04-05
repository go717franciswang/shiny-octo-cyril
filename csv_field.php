<?php

class CsvField
{
    public $column_mappers = array();
    public $reducers = array();
    public $final_mapper_key;
    public $alias;

    public function __construct($alias, $mapper=array())
    {
        $this->alias = $alias;
        $this->load_mapper($mapper);
    }

    private function load_mapper($mapper, $computation_level=0)
    {
        if (!empty($mapper)) {
            $func = $mapper[0];
            $args = array_slice($mapper, 1);

            foreach ($args as &$arg) {
                if (is_array($arg)) {
                    $arg = $this->load_mapper($arg, $computation_level+1);
                }
            }

            if ($func == 'LOWER') {
                $func = 'strtolower';
            } elseif ($func == 'UPPER') {
                $func = 'strtoupper';
            } elseif ($func == 'GREATEST') {
                $func = 'max';
            } elseif ($func == 'LEAST') {
                $func = 'min';
            } elseif ($func == '+') {
                $func = 'CsvColumnMappers::add';
            } elseif ($func == '-') {
                $func = 'CsvColumnMappers::subtract';
            } elseif ($func == '*') {
                $func = 'CsvColumnMappers::multiply';
            } elseif ($func == '/') {
                $func = 'CsvColumnMappers::divide';
            }

            $transformer = $args;
            array_unshift($transformer, $func);
            #TODO is there a better way to link the results of mappers and reducers?
            # this looks like a hack
            $key = md5(strtolower(print_r($transformer,1)));

            if ($computation_level == 0) {
                $this->final_mapper_key = $key;
            }

            if (in_array($func, array('SUM','MIN','MAX'))) {
                $this->reducers[$key] = $transformer;
                return array('REDUCER', $key);
            } else {
                $this->column_mappers[$key] = $transformer;
                return array('COLUMN_MAPPER', $key);
            }
        }
    }
}
