clear;

f12_pact_tp = [0.2297, 0.2027, 0.1862, 0.1753, 0.1674, 0.1711];       % pact_tp (with logging)
f12_pact_delta = [0.1155, 0.0981, 0.0874, 0.0898, 0.0876, 0.0881];    % pact_delta_tp
f12_act_tp = [0.2126, 0.1785, 0.1604, 0.1255, 0.0907, 0.0205];        % s2pl_tp (with logging)
f12_act_delta = [0.1988, 0.1930, 0.1607, 0.1208, 0.1022, 0.0169];     % s2pl_delta_tp
f12_abort = [0.0024, 0.0100, 0.0592, 0.2729, 0.6078, 0.9369];
f12_nt_tp = [31556, 21679, 13222, 7416, 3925, 1950];

f14_tp_pact = [4531, 4559, 4766, 5364, 5981];                         % PACT
f14_tp_act = [3797, 3208, 2734, 1883, 1534];                          % S2PL
f14_tp_orleans = [1813, 958, 651, 379, 173];                          % OrleansTxn
f14_tp_noDL = [1646, 1631, 1514, 1173, 1076];                         % OrleansTxn (no deadlock)
f14_act_abort = [0.0048 0.1622 0.2269 0.3799 0.5402];
f14_orleans_abort = [0.0000 0.0290 0.0268 0.0607 0.1460];

f16a_act = [0	40	401	999	1835	2597	3797	0	13	124	325	775	1909	3208	0	6	64	193	796	1688	2734	0	1	6	80	405	1109	1883	0	0	2	21	146	552	1534];
f16a_pact = [4531	4414	3825	3182	1923	935	0	4559	4546	4023	3363	2235	1062	0	4766	4606	4079	3158	2054	956	0	5364	4649	2606	1953	1396	791	0	5981	3794	1722	924	768	572	0];
f16b_pact_50 = [12	13	14	17	31	61	NaN	NaN	13	13	14	17	21	8	NaN	NaN	12	12	13	16	6	3	NaN	NaN	11	12	17	6	3	2	NaN	NaN	10	12	32	7	3	2	NaN];
f16b_pact_90 = [19	19	22	28	53	98	NaN	NaN	23	23	26	32	44	17	NaN	NaN	21	22	26	36	15	6	NaN	NaN	17	21	41	29	7	3	NaN	NaN	15	36	61	48	29	4	NaN];
f16b_act_50 = [NaN	9	9	9	13	18	13	NaN	NaN	6	7	7	4	4	4	NaN	NaN	6	6	3	3	2	2	NaN	NaN	6	5	2	2	1	1	NaN	NaN	9	8	3	2	2	1];
f16b_act_90 = [NaN	15	18	21	38	50	34	NaN	NaN	13	17	18	14	9	7	NaN	NaN	14	15	11	6	4	3	NaN	NaN	20	29	6	3	2	2	NaN	NaN	24	36	7	3	2	2];
f16c_act_RW = [0.0000	0.0000	0.0001	0.0002	0.0013	0.0025	0.0048	0.0000	0.0000	0.0000	0.0028	0.0167	0.0693	0.0866	0.1622	0.0000	0.0000	0.0000	0.0063	0.0315	0.0697	0.1369	0.2269	0.0000	0.0000	0.0001	0.0129	0.0434	0.1210	0.2323	0.3799	0.0000	0.0000	0.0002	0.0170	0.0618	0.1675	0.3245	0.5402 0];
f16c_act_DL = [0.0000	0.0000	0.0000	0.0000	0.0000	0.0000	0.0000	0.0000	0.0000	0.0000	0.0005	0.0034	0.0077	0.0026	0.0000	0.0000	0.0000	0.0001	0.0028	0.0117	0.0060	0.0024	0.0000	0.0000	0.0000	0.0011	0.0251	0.0260	0.0168	0.0054	0.0000	0.0000	0.0000	0.0042	0.0545	0.0699	0.0631	0.0264	0.0000 0];
f16c_act_SE_not = [0.0000	0.0008	0.0070	0.0165	0.0301	0.0415	0.0000	0.0000	0.0000	0.0069	0.0690	0.1574	0.2486	0.2062	0.0000	0.0000	0.0000	0.0083	0.0751	0.1554	0.2269	0.1700	0.0000	0.0000	0.0000	0.0068	0.0498	0.1372	0.2074	0.1618	0.0000	0.0000	0.0000	0.0035	0.0206	0.0823	0.1550	0.1496	0.0000 0];
f16c_act_SE_sure = [0.0000	0.0000	0.0000	0.0000	0.0000	0.0000	0.0000	0.0000	0.0000	0.0001	0.0012	0.0029	0.0042	0.0008	0.0000	0.0000	0.0000	0.0004	0.0035	0.0066	0.0030	0.0009	0.0000	0.0000	0.0000	0.0016	0.0090	0.0127	0.0082	0.0029	0.0000	0.0000	0.0000	0.0017	0.0057	0.0161	0.0176	0.0085	0.0000 0];

f17a1_100pact_tp = [4531, 8832, 15430, 28196];
f17a1_100pact_sd = [113, 148, 990, 2288];
f17a1_90pact_tp = [4455, 8819, 14857, 28662];
f17a1_90pact_sd = [159, 597, 1438, 2520];
f17a1_0pact_tp = [3797, 8778, 14805, 26733];
f17a1_0pact_sd = [158, 75, 1026, 2469];
f17a1_nt_tp = [24, 42, 74, 137];

f17a2_100pact_tp = [5169, 9814, 17032, 35748];
f17a2_100pact_sd = [108, 90, 835, 3951];
f17a2_90pact_tp = [4972, 8978, 14104, 26449];
f17a2_90pact_sd = [144, 368, 1037, 2817];
f17a2_0pact_tp = [3177, 6168, 8894, 14839];
f17a2_0pact_sd = [51, 488, 548, 873];
f17a2_nt_tp = [22, 41, 70, 128];

f17b1_100pact_tp = [783, 1479, 2792, 4747];
f17b1_100pact_sd = [28, 38, 120, 448];
f17b1_90pact_tp = [671, 1313, 2269, 3909];
f17b1_90pact_sd = [5, 32, 166, 366];
f17b1_0pact_tp = [581, 1153, 1857, 3631];
f17b1_0pact_sd = [5, 12, 41, 38];
f17b1_nt_tp = [6, 12, 20, 34];

f17b2_100pact_tp = [667, 1368, 2559, 5328];
f17b2_100pact_sd = [7, 38, 70, 539];
f17b2_90pact_tp = [638, 1153, 1918, 3199];
f17b2_90pact_sd = [20, 39, 71, 91];
f17b2_0pact_tp = [596, 980, 1672, 2553];
f17b2_0pact_sd = [1, 42, 58, 62];
f17b2_nt_tp = [7, 12, 20, 35];