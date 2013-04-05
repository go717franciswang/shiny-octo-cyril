<?php

require_once dirname(__FILE__) . "/../csv_field.php";

class CsvFieldTest extends PHPUnit_Framework_TestCase
{
    public function testAlias()
    {
        $field = new CsvField('car');
        $this->assertEquals($field->alias, 'car');
    }
    
    public function testColumnMappers()
    {
        $field = new CsvField('car', array('LOWER', 'car'));
        $this->assertEquals(array_values($field->column_mappers), array(array('strtolower', 'car')));
    }

    public function testReducers()
    {
        $field = new CsvField('distance', array('SUM', 'distance'));
        $this->assertEquals(array_values($field->reducers), array(array('SUM', 'distance')));
    }

    public function testColumnMappersInsideReducers()
    {
        $field = new CsvField('distance', array('SUM', array('ABS', 'distance')));
        $this->assertEquals(array_values($field->column_mappers), array(array('ABS', 'distance')));
        $keys = array_keys($field->column_mappers);
        $this->assertEquals(array_values($field->reducers), array(
            array('SUM', array('COLUMN_MAPPER', $keys[0]))
        ));
    }

    public function testReducersOutsiderColumnMappers()
    {
        $field = new CsvField('speed', array('/', array('SUM', 'distance'), array('SUM', 'time_spent')));
        $this->assertEquals(array_values($field->reducers), array(
            array('SUM', 'distance'), 
            array('SUM', 'time_spent'),
        ));
        $keys = array_keys($field->reducers);
        $this->assertEquals($field->column_mappers[$field->final_mapper_key], 
            array('CsvColumnMappers::divide', array('REDUCER', $keys[0]), array('REDUCER', $keys[1]))
        );
    }
}
