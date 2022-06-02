# run silo
$numSilo = 2
Start-Process "dotnet" -ArgumentList "run --project SnapperSiloHost $numSilo"