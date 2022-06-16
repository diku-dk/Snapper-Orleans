#Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

function RunFirstExperiment 
{
    param ( [int] $experimentID, [int] $numCPU, [string] $implementation, [string] $benchmark, [string] $loggingEnabled, [int] $numOrderGrainPerDistrict)
    Start-Process "dotnet" -ArgumentList "run --project SnapperSiloHost $numCPU $implementation $loggingEnabled"
    Start-Sleep 150
    Wait-Process -Name "SnapperSiloHost"
    Start-Sleep 5
}

function RunExperiment 
{
    param ( [int] $experimentID, [int] $numCPU, [string] $implementation, [string] $benchmark, [string] $loggingEnabled, [int] $numOrderGrainPerDistrict)
    Start-Process "dotnet" -ArgumentList "run --project SnapperSiloHost $numCPU $implementation $loggingEnabled"
    Start-Sleep 30
    Wait-Process -Name "SnapperSiloHost"
    Start-Sleep 5
}

function HasNugetSource ($url)
{
    return [bool](dotnet nuget source | ? { $_ -like "*$url"})
}

$flag = $false
if ($flag)
{
    RunFirstExperiment 12 4 "NONTXN" "SMALLBANK" "false" 0
    RunExperiment 12 4 "SNAPPER" "SMALLBANK" "false" 0
    RunExperiment 12 4 "SNAPPER" "SMALLBANK" "true" 0

    RunExperiment 14 4 "SNAPPER" "SMALLBANK" "true" 0
    RunExperiment 14 4 "ORLEANSTXN" "SMALLBANK" "true" 0

    RunExperiment 16 4 "SNAPPER" "SMALLBANK" "true" 0

    RunExperiment 17 4 "NONTXN" "SMALLBANK" "false" 0
    RunExperiment 17 8 "NONTXN" "SMALLBANK" "false" 0
    RunExperiment 17 16 "NONTXN" "SMALLBANK" "false" 0
    RunExperiment 17 32 "NONTXN" "SMALLBANK" "false" 0

    RunExperiment 17 4 "SNAPPER" "SMALLBANK" "true" 0
    RunExperiment 17 8 "SNAPPER" "SMALLBANK" "true" 0
    RunExperiment 17 16 "SNAPPER" "SMALLBANK" "true" 0
    RunExperiment 17 32 "SNAPPER" "SMALLBANK" "true" 0

    RunExperiment 17 4 "NONTXN" "TPCC" "false" 2
    RunExperiment 17 8 "NONTXN" "TPCC" "false" 2
    RunExperiment 17 16 "NONTXN" "TPCC" "false" 2
    RunExperiment 17 32 "NONTXN" "TPCC" "false" 2

    RunExperiment 17 4 "NONTXN" "TPCC" "false" 1
    RunExperiment 17 8 "NONTXN" "TPCC" "false" 1
    RunExperiment 17 16 "NONTXN" "TPCC" "false" 1
    RunExperiment 17 32 "NONTXN" "TPCC" "false" 1

    RunExperiment 17 4 "SNAPPER" "TPCC" "true" 2
    RunExperiment 17 8 "SNAPPER" "TPCC" "true" 2
    RunExperiment 17 16 "SNAPPER" "TPCC" "true" 2
    RunExperiment 17 32 "SNAPPER" "TPCC" "true" 2

    RunExperiment 17 4 "SNAPPER" "TPCC" "true" 1
    RunExperiment 17 8 "SNAPPER" "TPCC" "true" 1
    RunExperiment 17 16 "SNAPPER" "TPCC" "true" 1
    RunExperiment 17 32 "SNAPPER" "TPCC" "true" 1
}
else
{
    $currentPath = GET-LOCATION
    $sourcePath = [string]$currentPath + "\MyNuGet"
    if (HasNugetSource $sourcePath -eq $false)
    {
        dotnet nuget add source $sourcePath -n "MyNuGet"
        Write-Output "add a new source for nuget"
    }

    RunFirstExperiment 15 4 "SNAPPER" "NEWSMALLBANK" "false" 0
    RunExperiment 15 4 "ORLEANSTXN" "NEWSMALLBANK" "false" 0
}