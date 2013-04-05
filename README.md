Example usage
-------------

#Instantiation
`$query = new CsvQuery;`

#Simple select clause
    $distance = CsvQuery::Field('distance');
    $query->select(array($distance))
          ->from('trips.csv');
    $query->execute();

#Select clause with transformation
    $car_lower = CsvQuery::Field('car', array('LOWER', 'car'));
    $distance = CsvQuery::Field('distance', array('ABS', 'distance'));
    $query->select(array($car_lower, $distance));

#Select clause with nested transformation
    $speed = CsvQuery::Field('speed', array('/',
        array('ABS', 'distance'),
        'time_spent',
    ));

#Where clause
    $car = CsvQuery::Field('car');
    $query->select(array($distance))
          ->from('trips.csv')
          ->where(array('=', $car, 'Toyota'));
or `->where(array('IN', $car, array('Toyota', 'Ford')));`

#Group by clause
    $car = CsvQuery::Field('car');
    $speed = CsvQuery::Field('speed', array('/', 
        array('SUM', 'distance'), 
        array('SUM', 'time_spent')
    ));
    $query->select(array($car, $speed))
          ->from('trips.csv')
          ->group_by(array($car));
