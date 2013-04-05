# TODO update readme
# example usage
$campaign_id = CsvQuery::Field('campaign_id');
$kwd = CsvQuery::Field('okkw', array(
    'mappers' => array('TRIM', 'LOWER'), 
    'alias' => 'kwd'
));
$sst_in = CsvQuery::Field('clicks_incoming', array(
    'reducers' => array('SUM'),
    'alias' => 'sst_in',
));

$query = new CsvQuery
    ->select(array($campaign_id, $kwd, $sst_in))
    ->from($filename)
    ->where(array(
        array($campaign_id, 'IN', $campaign_ids), 
        'AND',
        array($kwd, '!=', 'foo'),
    )))
    ->group_by(array($campaign_id, $kwd));

$result = $query->execute();

$result = array_like_object(
  row('campaign_id' => .., 'kwd' => .., 'sst_in' => ..),
  row('campaign_id' => .., 'kwd' => .., 'sst_in' => ..),
  ...
);
