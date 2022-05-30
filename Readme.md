# Snapper-Sigmod-2022
Snapper: A Transaction Library for Actor Systems

### Code Structure
`Concurrency.Implementation` contains the implementation of key components of Snapper, including `Coordinator` and `Transactional actor` discussed in the paper. The code files are named as `LocalCoordinatorGrain.cs` and `TransactionExecutionGrain.cs` respectively. And `Persist.Grains/PersistSingleton.cs` implements the `Logger`.

`SmallBank` and `TPCC` are two benchmarks we used. `SmallBank.Grains` and `TPCC.Grains` contain the transaction logic of user-defined actors, aka grains in Orleans.

### Software Dependency
Snapper is developed on **.NET Core** platform.

### Environment Configuration
Set `localCluster` in `Utilities/Constants.cs` as `true` to run experiment on a local machine.

### Workload Setting
Customise your workload by changing `<BenchmarkConfig>` settings in `SnapperExperimentController/App.config`.

### Build and Run
Use the following command to start the server:
`dotnet run --project SiloHost/SiloHost.csproj`

To build and run our sample client, run the following two projects one after the other:

`dotnet run --project SnapperExperimentController/SnapperExperimentController.csproj`

`dotnet run --project SnapperExperimentProcess/SnapperExperimentProcess.csproj`