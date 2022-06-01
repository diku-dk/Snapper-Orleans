# run silo
for ($siloID = 0; $siloID -le 8; $siloID++)
{
    Start-Process "dotnet" -ArgumentList "run --project SnapperSiloHost $siloID"
    Start-Sleep 5
}