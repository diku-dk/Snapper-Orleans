# run silo
$siloID = 8
Start-Process "dotnet" -ArgumentList "run --project SnapperSiloHost $siloID"