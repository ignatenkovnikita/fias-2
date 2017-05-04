include 'fias.php';
$dbconfig = ['host' => 'localhost', 'username' => 'root', 'password' => '', 'dbname' => 'fias'];
$fias = new fias(__DIR__.'/data/', $dbconfig);
//$fias->debug = true;
$fias->import();
