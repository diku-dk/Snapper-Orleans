===================================================================

README for reproducibility submission
- Paper ID: 457
- Paper title: Hybrid Deterministic and Nondeterministic Execution of Transactions in Actor Systems

# Snapper-Orleans (SIGMOD 2022)
Snapper is a transaction library currently built on top of Orleans. Snapper exploits the characteristics of actor-oriented programming to improve the performance of multi-actor transactions by employing deterministic transaction execution. where pre-declared actor access information is used to generate deterministic execution schedules. 

Snapper can also execute actor transactions using conventional nondeterministic strategies, including S2PL, to account for scenarios where actor access information cannot be pre-declared. 

A salient feature of Snapper is the ability to execute concurrent hybrid workloads, where some transactions are executed deterministically while the others are executed nondeterministically.

### Source Code
The source code of Snapper can be downloaded here: [Snapper source code](https://github.com/diku-dk/Snapper-Orleans).

`Concurrency.Implementation` contains the implementation of key components of Snapper, including `Coordinator` and `Transactional actor` discussed in the paper. The code files are named as `GlobalTransactionCoordinatorGrain.cs` and `TransactionExecutionGrain.cs` respectively. And `Persist.Grains\PersistSingleton.cs` implements the `Logger`. 

`SmallBank` and `TPCC` are two benchmarks we used. `SmallBank.Grains` and `TPCC.Grains` contain the transaction logic of user-defined actors. For example, the `SmallBank.Grains\CustomerAccountGroupGrain.cs` extends the base class `TransactionExecutionGrain` in order to get transactional guarantees provided by Snapper. 

`SnapperSiloHost` is used to configure and start the server process. `SnapperExperimentProcess` contains the client program that sends transaction requests to the server.

### Software Dependency
- SDK: Snapper is developed with **C#** on **.NET** platform. To compile and run the source code, **.NET Core SDK 3.1.301** should be installed.
- NuGet packages: The NuGet packages and corresponding versions are listed in each `*.csproj` file. **Orleans 3.4.3** is the one we used for running experiments.

### Cloud Deployment
- Server: The server side is an **Orleans Cluster** which contains 1 silo. The server is deployed on an **AWS EC2 instance**.
- Cluster membership table: the **Orleans Cluster** membership information is stored in a **DynamoDB** table. To get access to **DynamoDB**, the **ServiceRegion**, **AccessKey** and **SecretKey** should be provided.
- Client: We use **Orleans Client**s to send transaction requests to the server. In our experiments, `SnapperExperimentProcess` is deployed on another **AWS EC2 instance**.
- Both **EC2 instance**s should co-locate in the same region and availability zone.

### Datasets
This section explains how the transaction workloads are generated and used.
- Uniform & Hotspot distribution: Both types of workloads are generated on-the-fly when the `SnapperExperimentProcess` is started.
- Zipfian distribution: This workload needs to be generated in advance. In our client program, the zipfian distributed workloads are read from pre-generated data files. The data generator can be found in `SmallBank.DataGenerator`.
  * Run the data generator: `dotnet run --project SmallBank.DataGenerator\SmallBank.DataGenerator.csproj`
  * The generated dataset can be found in the folder `data\zipfian_workload`.

### How To Run Experiments
Follow the steps below to re-produce all the experimental results presented in the paper.

##### Prepare the virtual machines
1. Start a Server machine and a Client machine on AWS Cloud Platform.
   - Both VMs should choose AMI **Microsoft Windows Server 2019 Base**.
   - Specification for Server VM: **c5n.9xlarge** (36-core, 96GB memory), 128GB **io2 SSD**, 63999 IOPS
   - Specification for Client VM: **c5n.9xlarge** (36-core, 96GB memory), 40GB **gp2 SSD**
   - Both VMs should locate in the same **region** and same **subnet** (eg. us-east-2a), and put in the same [**placement group**](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html).
   - Configure the **security group** and make sure all network traffic is allowed between these two VMs.
2. Connect to and configure both VMs.
   - Turn off **IE Enhanced Security Configuration** in **Server Manager** ==> **Local Server**.
   - Turn off firewall.
3. Install [**git**](https://git-scm.com/downloads) on both VMs.
4. Install [**.NET Core SDK 3.1.301**](https://github.com/dotnet/core/blob/main/release-notes/3.1/3.1.5/3.1.301-download.md) on both VMs.

##### Prepare the source code and dataset
1. Download the [Snapper source code (main branch)](https://github.com/diku-dk/Snapper-Orleans) to both Server machine and Client machine.
   - `git clone https://github.com/diku-dk/Snapper-Orleans.git`.
2. Create the file `AWS_credential.txt`, which should contain 3 lines: **ServiceRegion** (eg. us-east-2), **AccessKey**, **SecretKey**.
3. Put the `AWS_credential.txt` file into the `Snapper-Orleans` folder on both VMs.
4. Run the data generator
   - `dotnet run --project SmallBank.DataGenerator\SmallBank.DataGenerator.csproj`.
   - It will take around 5min to complete.

##### Run the experiments (Fig.12, 13, 14, 16, 17)
1. Run the powershell script on the Server VM to start the server: `.\RunSilo.ps1`.
2. Run the powershell script on the Client VM to start the client: `.\RunClient.ps1`.
   - Both powershell scripts are included in the source code folder.
   - The client should be started without waiting for the server side script to complete.
3. It will take around 4 hours to complete.
4. Check all the results in `data\result.txt` stored on the Client machine.

##### Run the experiments (Fig.15)
0. Switch the source code on both VMs to another git branch: `git checkout -b InvOrleansTxn`.
   - The difference between the **main** and **InvOrleansTxn** branches is that, in **InvOrleansTxn** branch, we made some changes to the Orleans source code so to collect some time intervals while executing an Orleans transaction.
1. Run the powershell script on the Server VM to start the server: `.\RunSilo.ps1`.
2. Run the powershell script on the Client VM to start the client: `.\RunClient.ps1`.
   - Both powershell scripts are included in the source code folder.
   - The client should be started without waiting for the server side script to complete.
3. It will take around 10min to complete.
4. Check all the results in `data\breakdown_latency.txt` stored on the Client machine.
5. In the result file, each row contains the breakdown latency of a single transaction. Each row has 9 values that represent 9 time intervals as described in the paper (Fig.15). To get the results shown in Fig.15, we need to first filter out abnormal data points (outliers) such that, for each time interval, `standard_deviation < 20% * average`. Then we should calculate the average value for each of the time intervals.

===================================================================
