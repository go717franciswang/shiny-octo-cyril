<?php

class CsvColumnMappers
{
    public static function add()
    {
        return array_sum(func_get_args());
    }

    public static function subtract()
    {
        $args = func_get_args();
        return $args[0] - array_sum(array_slice($args,1));
    }

    public static function multiply()
    {
        $args = func_get_args();
        $product = 1;
        foreach ($args as $arg) {
            $product *= $arg;
        }
        return $product;
    }

    public static function divide()
    {
        $args = func_get_args();
        $division = $args[0];
        foreach (array_slice($args,1) as $arg) {
            $division /= $arg;
        }
        return $division;
    }

    public static function equal($a, $b)
    {
        return $a == $b;
    }

    public static function not_equal($a, $b)
    {
        return $a != $b;
    }

    public static function in($a, $b)
    {
        return in_array($a, $b);
    }
}
