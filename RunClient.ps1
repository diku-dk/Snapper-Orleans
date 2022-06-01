# run controller
Start-Process "dotnet" -ArgumentList "run --project SnapperExperimentController"
Start-Sleep 60

# run worker
for ($workerID = 0; $workerID -le 7; $workerID++)
{
    Start-Process "dotnet" -ArgumentList "run --project SnapperExperimentProcess $workerID"
}