# run silo
$siloID = 0
Start-Process "dotnet" -ArgumentList "run --project SnapperSiloHost $siloID"
Start-Sleep 60

$numSilo = 8
for ($siloID = 1; $siloID -le $numSilo - 1; $siloID++)
{
    Start-Process "dotnet" -ArgumentList "run --project SnapperSiloHost $siloID"
    Start-Sleep 5
}