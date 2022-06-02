# run silo
$numSilo = 8
Start-Process "dotnet" -ArgumentList "run --project SnapperSiloHost $numSilo"