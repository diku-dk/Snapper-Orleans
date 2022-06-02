# run controller
Start-Process "dotnet" -ArgumentList "run --project SnapperExperimentController"
Start-Sleep 60

$numSilo = 2
# run worker
for ($workerID = 0; $workerID -le $numSilo - 1; $workerID++)
{
    Start-Process "dotnet" -ArgumentList "run --project SnapperExperimentProcess $workerID"
}