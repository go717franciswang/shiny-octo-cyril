<?php

require_once dirname(__FILE__) . "/../csv_query.php";

class CsvQueryTest extends PHPUnit_Framework_TestCase
{
    public function setUp()
    {
        $this->query = new CsvQuery; 
    }

    public function testFieldGenerateValidFieldObject()
    {
        $field = CsvQuery::Field('distance');

        $this->assertEquals($field->alias, 'distance');
    }

    public function testExecuteThrowsExceptionWhenSelectIsMissing()
    {
        $this->setExpectedException('IncompleteStatement', 'Select is missing');

        $this->query->execute();
    }

    public function testExecuteThrowsExceptionWhenFromIsMissing()
    {
        $this->setExpectedException('IncompleteStatement', 'From is missing');

        $distance = CsvQuery::Field('distance');
        $this->query->select(array($distance));
        $this->query->execute();
    }

    public function testCsvHeadersAreRead()
    {
        $distance = CsvQuery::Field('distance');
        $this->query->select(array($distance))
                    ->from(dirname(__FILE__) . '/trips.csv');
        $this->query->execute();
        $this->assertEquals($this->query->headers, array('car', 'distance', 'time_spent'));
    }

    public function testQuery()
    {
        $distance = CsvQuery::Field('distance');
        $this->query->select(array($distance))
                    ->from(dirname(__FILE__) . '/trips.csv');
        $result = $this->query->execute();
        $this->assertEquals($result, array(
            array(-20),
            array(40),
            array(40),
        ));
    }

    public function testQueryWithColumnMapperAbs()
    {
        $distance = CsvQuery::Field('distance', array('ABS', 'distance'));
        $this->query->select(array($distance))
                    ->from(dirname(__FILE__) . '/trips.csv');
        $result = $this->query->execute();
        $this->assertEquals($result, array(
            array(20),
            array(40),
            array(40),
        ));
    }

    public function testQueryWithColumnMapperLower()
    {
        $car = CsvQuery::Field('car', array('LOWER', 'car'));
        $this->query->select(array($car))
                    ->from(dirname(__FILE__) . '/trips.csv');
        $result = $this->query->execute();
        $this->assertEquals($result, array(
            array('toyota'),
            array('toyota'),
            array('ford'),
        ));
    }

    public function testQueryWithColumnMapperDivide()
    {
        $speed = CsvQuery::Field('speed', array('/', 'distance', 'time_spent'));
        $this->query->select(array($speed))
                    ->from(dirname(__FILE__) . '/trips.csv');
        $result = $this->query->execute();
        $this->assertEquals($result, array(
            array(-4),
            array(10),
            array(5),
        ));
    }

    public function testQueryWithColumnMapperSumAndGroupBy()
    {
        $car = CsvQuery::Field('car', array('LOWER', 'car'));
        $distance = CsvQuery::Field('distance', array('SUM', array('ABS', 'distance')));

        $this->query->select(array($car, $distance))
            ->from(dirname(__FILE__) . '/trips.csv')
            ->group_by(array($car));
        $result = $this->query->execute();
        $this->assertEquals($result, array(
            array('toyota', 60),
            array('ford', 40),
        ));
    }
}
