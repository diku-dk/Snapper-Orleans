% set up color
color_pact = [29/255, 141/255, 238/255];
color_act = [252/255, 166/255, 23/255];
color_act_line = [237/255, 107/255, 18/255];
color_orleans_line = [105/255, 109/255, 114/255];
color_orleans = [0, 0, 0];

color_RW = [0,131/255,64/255];
color_DL = [0,0,0];
color_SE_not = [107/255, 192/255, 90/255];
color_SE_sure = [1,1,1];

color_skew = [70/255, 70/255, 70/255];
marker_color_skew = [0, 0, 0];

scale = [4, 8, 16, 32];

% ======================================================================== Figure 12 ======================================================================== %
f12 = figure;
hold on;
set(gca,'FontName','Times', 'FontSize', 10);

f12_tp(6, 2, 2) = 0;
f12_tp(:, 1, 1) = f12_pact_tp;
f12_tp(:, 1, 2) = f12_pact_delta;
f12_tp(:, 2, 1) = f12_act_tp;
f12_tp(:, 2, 2) = f12_act_delta;

yyaxis left
groupLabels = {2, 4, 8, 16, 32, 64};
NumGroupsPerAxis = size(f12_tp, 1);
NumStacksPerGroup = size(f12_tp, 2);
% Count off the number of bins
groupBins = 1:NumGroupsPerAxis;
MaxGroupWidth = 0.5; % Fraction of 1. If 1, then we have all bars in groups touching
groupOffset = MaxGroupWidth/NumStacksPerGroup;
for i = 1:NumStacksPerGroup
    Y12 = squeeze(f12_tp(:,i,:));
    
    % Center the bars:
    internalPosCount = i - ((NumStacksPerGroup+1) / 2);
    
    % Offset the group draw positions:
    groupDrawPos = (internalPosCount)* groupOffset + groupBins;
    
    h12(i,:) = bar(Y12, 'stacked');
    set(h12(i,:),'BarWidth',groupOffset);
    set(h12(i,:),'XData',groupDrawPos + (i - 1) * 0.05);
    
    % Set bar color
    b = h12(i,:);
    if (i == 1) 
        b(1).FaceColor = color_pact;
        b(1).EdgeColor = color_pact;
        b(2).EdgeColor = color_pact;
        b(2).LineStyle = '-';
    end
    if (i == 2)
        b(1).FaceColor = color_act;
        b(1).EdgeColor = color_act;
        b(2).EdgeColor = color_act;
        b(2).LineStyle = '--';
    end
    b(1).LineWidth = 1.5;
    b(2).LineWidth = 1.5;
    b(2).FaceColor = [1,1,1];
end
hold off;
set(gca,'XTickMode','manual');
set(gca,'XTick',1:NumGroupsPerAxis);
set(gca,'XTickLabelMode','manual');
set(gca,'XTickLabel',groupLabels);

set(gca, 'xcolor', 'black', 'ycolor', 'black');
ylim([0 0.5]);
yticks([0 0.1 0.2 0.3 0.4 0.5]);
set(gca, 'ycolor', 'black');
ylabel('Relative throughput');

yyaxis right
plot(f12_abort, 'color', color_act_line, 'Marker', 'o', 'LineStyle', '--', 'LineWidth', 1.5);
ylim([0 1]);
yticks([0 0.2 0.4 0.6 0.8 1]);
yticklabels({'0%', '20%', '40%', '60%', '80%', '100%'});
set(gca, 'ycolor', 'black');

xlim([0.5 6.5]);
xticks([0.5 1 2 3 4 5 6 6.5]);
xticklabels({'', '2', '4', '8', '16', '32', '64', ''});
set(gca, 'xcolor', 'black');
leg = legend('PACT (CC + Logging)', 'PACT (CC)', 'ACT (CC + Logging)', 'ACT (CC)', 'ACT abort rate (CC + Logging)');
set(leg, 'Location', 'north');
set(leg, 'FontSize', 10);
set(leg, 'NumColumns', 1);
set(gcf,'Position',[400, 300, 800, 600]);
ylabel('Abort rate');
xlabel('Transaction size');

% set up the secondary x-axis
primary_axis = gca;
secondary_axis = axes('Position',primary_axis.Position,'XAxisLocation','top','Color','none', 'FontName','Times', 'FontSize', 10);
secondary_axis.XLim = [0.5 6.5];
secondary_axis.XTick = [0.5 1 2 3 4 5 6 6.5];
secondary_axis.XTickLabel = {'', string(f12_nt_tp(1)), string(f12_nt_tp(2)), string(f12_nt_tp(3)), string(f12_nt_tp(4)), string(f12_nt_tp(5)), string(f12_nt_tp(6)), ''};
secondary_axis.XLabel.String = 'NT throughput [txn/sec]';
% remove the y-labels
secondary_axis.YTickLabel = [];

print(gcf,'-r500','-dpdf','Figure12.pdf');

% ======================================================================== Figure 14 ======================================================================== %
f14 = figure;
hold on;
set(gca,'FontName','Times', 'FontSize', 10);

f14_tp(5, 4, 1) = 0;
f14_tp(:, 1, 1) = f14_tp_pact;
f14_tp(:, 2, 1) = f14_tp_act;
f14_tp(:, 3, 1) = f14_tp_orleans;
f14_tp(:, 4, 1) = f14_tp_noDL;

yyaxis left
set(gca, 'ycolor', 'black');
groupLabels = {'uniform', 'low', 'medium', 'high', 'very high'};
NumGroupsPerAxis = size(f14_tp, 1);
NumStacksPerGroup = size(f14_tp, 2);
groupBins = 1:NumGroupsPerAxis;   % Count off the number of bins
MaxGroupWidth = 0.5;    % Fraction of 1. If 1, then we have all bars in groups touching
groupOffset = MaxGroupWidth/NumStacksPerGroup;
for i = 1:NumStacksPerGroup
    Y14 = squeeze(f14_tp(:,i,:));
    internalPosCount = i - ((NumStacksPerGroup+1) / 2);  % Center the bars
    groupDrawPos = (internalPosCount)* groupOffset + groupBins;  % Offset the group draw positions
    
    h14(i,:) = bar(Y14, 'stacked');
    set(h14(i,:),'BarWidth',groupOffset);
    set(h14(i,:),'XData',groupDrawPos + (i - 1) * 0.05);
    
    % Set bar color
    b = h14(i,:);
    if (i == 1) 
        b(1).FaceColor = color_pact;
        b(1).EdgeColor = color_pact;
    end
    if (i == 2)
        b(1).FaceColor = color_act;
        b(1).EdgeColor = color_act;
    end
    if (i == 3 || i == 4)
        b(1).FaceColor = color_orleans;
        b(1).EdgeColor = color_orleans;
    end
    if (i == 4)
        b(1).FaceColor = [1,1,1];
        b(1).EdgeColor = color_orleans;
    end
    b(1).LineWidth = 1.5;
end
hold off;
set(gca,'XTickMode','manual');
set(gca,'XTick',1:NumGroupsPerAxis);
set(gca,'XTickLabelMode','manual');
set(gca,'XTickLabel',groupLabels);

ylabel('Throughput [txn/sec]');
ylim([0 7000]);
yticks([0 1000 2000 3000 4000 5000 6000 7000]);

yyaxis right
hold on;
plot(f14_act_abort, 'color', color_act_line, 'Marker', 'o', 'LineStyle', '--', 'LineWidth', 1.5);
plot(f14_orleans_abort, 'color', color_orleans_line, 'Marker', '*', 'LineStyle', '--', 'LineWidth', 1.5);
ylim([0 1]);
yticks([0 0.2 0.4 0.6 0.8 1]);
yticklabels({'0%', '20%', '40%', '60%', '80%', '100%'});
ylabel('Abort rate');
set(gca, 'ycolor', 'black');

set(gca, 'xcolor', 'black', 'ycolor', 'black');
leg = legend('PACT', 'ACT', 'OrleansTxn', 'OrleansTxn (no deadlock)', 'ACT abort rate', 'OrleansTxn abort rate');
set(leg, 'Location', 'northwest');
set(leg, 'FontSize', 10);
set(leg, 'NumColumns', 1);
xlabel('Workload skewness');
set(gcf,'Position',[400, 300, 800, 600]);
print(gcf,'-r500','-dpdf','Figure14.pdf');

% ======================================================================== Figure 16 (a) ======================================================================== %
f16a = figure;
hold on;
set(gca,'FontName','Times', 'FontSize', 10);

f16a_data(5, 7, 2) = 0;
f16a_data(1:5, 1:7, 1) = reshape(f16a_act, [7, 5])';
f16a_data(1:5, 1:7, 2) = reshape(f16a_pact, [7, 5])';

groupLabels = {'uniform', 'low', 'median', 'high', 'very high'};
NumGroupsPerAxis = size(f16a_data, 1);
NumStacksPerGroup = size(f16a_data, 2);
groupBins = 1:NumGroupsPerAxis;  % Count off the number of bins
MaxGroupWidth = 0.5;  % Fraction of 1. If 1, then we have all bars in groups touching
groupOffset = MaxGroupWidth/NumStacksPerGroup;
for i = 1:NumStacksPerGroup
    Y16a = squeeze(f16a_data(:,i,:));
    internalPosCount = i - ((NumStacksPerGroup+1) / 2);  % Center the bars
    groupDrawPos = (internalPosCount)* groupOffset + groupBins;  % Offset the group draw positions
    
    h16a(i,:) = bar(Y16a, 'stacked');
    set(h16a(i,:),'BarWidth',groupOffset);
    set(h16a(i,:),'XData',groupDrawPos + (i - 1) * 0.05);
    
    % Set bar color
    b = h16a(i,:);
    if (i < 7)
        b(1).FaceColor = color_act;
        b(1).EdgeColor = color_act;
        b(2).FaceColor = color_pact;
        b(2).EdgeColor = color_pact;
    end
    if (i == 7)
        b(1).FaceColor = color_act;
        b(1).EdgeColor = color_act;
        b(2).FaceColor = color_act;
        b(2).EdgeColor = color_act;
    end
end
hold off;
set(gca,'XTickMode','manual');
set(gca,'XTick',1:NumGroupsPerAxis);
set(gca,'XTickLabelMode','manual');
set(gca,'XTickLabel',groupLabels);

set(gca, 'xcolor', 'black', 'ycolor', 'black');
leg = legend('ACT', 'PACT');
set(leg, 'Location', 'northwest');
set(leg, 'FontSize', 10);
set(leg, 'NumColumns', 2);
xlabel('Workload skewness');
ylabel('Throughput [txn/sec]');
ylim([0, 6000]);
yticks([0, 2000, 4000, 6000]);
yticklabels({'0', '2K', '4K', '6K'});
set(gcf,'Position',[400, 150, 800, 300]);
print(gcf,'-r500','-dpdf','Figure16a.pdf');

% ======================================================================== Figure 16 (b) ======================================================================== %
f16b = figure;
hold on;
set(gca,'FontName','Times', 'FontSize', 10);

plot(f16b_pact_90, 'color', color_pact, 'Marker', 'o', 'LineStyle', '--', 'LineWidth', 1.5);
plot(f16b_pact_50, 'color', color_pact, 'Marker', 'o', 'LineStyle', '-', 'LineWidth', 1.5);
plot(f16b_act_90, 'color', color_act, 'Marker', '*', 'LineStyle', '--', 'LineWidth', 1.5);
plot(f16b_act_50, 'color', color_act, 'Marker', '*', 'LineStyle', '-', 'LineWidth', 1.5);

set(gca, 'xcolor', 'black', 'ycolor', 'black');
ylim([0, 100]);
yticks([0 20 40 60 80 100]);
ylabel('Latency [milliseconds]');

xlim([0, 40]);
xticks([4 12 20 28 36]);
xticklabels({'uniform', 'low', 'medium', 'high', 'very high'});
xlabel('Workload skewness');

leg = legend('PACT: 90%', 'PACT: 50%', 'ACT: 90%', 'ACT: 50%');
set(leg, 'Location', 'north');
set(leg, 'FontSize', 10);
set(leg, 'NumColumns', 1);
set(gcf,'Position',[400, 150, 800, 300]);
print(gcf,'-r500','-dpdf','Figure16b.pdf');

% ======================================================================== Figure 16 (c) ======================================================================== %
f16c = figure;
hold on;
set(gca,'FontName','Times', 'FontSize', 10);

f16c_data(5, 8, 4) = 0;
f16c_data(1:5, 1:8, 1) = reshape(f16c_act_RW, [8, 5])';
f16c_data(1:5, 1:8, 2) = reshape(f16c_act_DL, [8, 5])';
f16c_data(1:5, 1:8, 3) = reshape(f16c_act_SE_not, [8, 5])';
f16c_data(1:5, 1:8, 4) = reshape(f16c_act_SE_sure, [8, 5])';

groupLabels = {'uniform', 'low', 'median', 'high', 'very high'};
NumGroupsPerAxis = size(f16c_data, 1);
NumStacksPerGroup = size(f16c_data, 2);
groupBins = 1:NumGroupsPerAxis;  % Count off the number of bins
MaxGroupWidth = 0.5;  % Fraction of 1. If 1, then we have all bars in groups touching
groupOffset = MaxGroupWidth/NumStacksPerGroup;
for i = 1:NumStacksPerGroup
    Y16c = squeeze(f16c_data(:,i,:));
    internalPosCount = i - ((NumStacksPerGroup+1) / 2);  % Center the bars
    groupDrawPos = (internalPosCount)* groupOffset + groupBins;  % Offset the group draw positions
    
    h16c(i,:) = bar(Y16c, 'stacked');
    set(h16c(i,:),'BarWidth',groupOffset);
    set(h16c(i,:),'XData',groupDrawPos + (i - 1) * 0.05);
    
    % Set bar color
    b = h16c(i,:);
    b(1).FaceColor = color_RW;
    b(2).FaceColor = color_DL;
    b(3).FaceColor = color_SE_not;
    b(4).FaceColor = color_SE_sure;
end
hold off;
set(gca,'XTickMode','manual');
set(gca,'XTick',1:NumGroupsPerAxis);
set(gca,'XTickLabelMode','manual');
set(gca,'XTickLabel',groupLabels);

set(gca, 'xcolor', 'black', 'ycolor', 'black');
ylim([0 1]);
yticks([0 0.2 0.4 0.6 0.8 1]);
yticklabels({'0%', '20%', '40%', '60%', '80%', '100%'});
leg = legend('(1) Read/Write conflict', '(2) Deadlock', '(3) Serializability Check (not sure)', '(4) Serializability Check (sure)');
set(leg, 'Location', 'northwest');
set(leg, 'FontSize', 10);
set(leg, 'NumColumns', 1);
xlabel('Workload skewness');
ylabel('Abort rate in all');
set(gcf,'Position',[400, 150, 800, 300]);
print(gcf,'-r500','-dpdf','Figure16c.pdf');

% ======================================================================== Figure 17 (a1) ======================================================================== %
f17a1 = figure;
set(gca,'FontName','Times', 'FontSize', 10);
hold on;

yyaxis left;
errorbar(scale,f17a1_100pact_tp,f17a1_100pact_sd, 'color', color_skew, 'Marker', 'o', 'MarkerFaceColor', marker_color_skew, 'MarkerEdgeColor', marker_color_skew, 'MarkerSize', 8, 'LineStyle', '-', 'LineWidth', 1.7);
errorbar(scale,f17a1_90pact_tp,f17a1_90pact_sd, 'color', color_skew, 'Marker', '^', 'MarkerFaceColor', marker_color_skew, 'MarkerEdgeColor', marker_color_skew, 'MarkerSize', 8, 'LineStyle', '--', 'LineWidth', 1.7);
errorbar(scale,f17a1_0pact_tp,f17a1_0pact_sd, 'color',color_skew, 'Marker', 's', 'MarkerFaceColor', marker_color_skew, 'MarkerEdgeColor', marker_color_skew, 'MarkerSize', 8, 'LineStyle', ':', 'LineWidth', 1.7);

set(gca, 'xcolor', 'black', 'ycolor', 'black');
ylim([0, 40000]);
yticks([0, 10000, 20000, 30000, 40000]);
yticklabels({'0', '10K', '20K', '30K', '40K'});
ylabel('Throughput [txn/sec]');

leg = legend('Uniform: 100% PACT', 'Uniform: 90% PACT', 'Uniform: 0% PACT');
set(leg, 'Location', 'northwest');
set(leg, 'FontSize', 10);
set(gcf,'Position',[400, 300, 800, 600]);

xlim([0, 35]);
xticks([4 8 16 32]);
xlabel('Number of CPUs');

yyaxis right;
ylim([0, 40000]);
yticks([0, 10000, 20000, 30000, 40000]);
yticklabels({'0', '10K', '20K', '30K', '40K'});
set(gca, 'xcolor', 'black', 'ycolor', 'black');

% set up the secondary x-axis
primary_axis = gca;
secondary_axis = axes('Position',primary_axis.Position,'XAxisLocation','top','YAxisLocation','right','Color','none', 'FontName','Times', 'FontSize', 10);
secondary_axis.XLim = [0 35];
secondary_axis.XTick = [4 8 16 32];
secondary_axis.XTickLabel = {string(f17a1_nt_tp(1)) + 'K', string(f17a1_nt_tp(2)) + 'K', string(f17a1_nt_tp(3)) + 'K', string(f17a1_nt_tp(4)) + 'K'};
secondary_axis.XLabel.String = 'NT throughput [txn/sec]';
% remove the y-labels
secondary_axis.YLim = [0, 40000];
secondary_axis.YTick = [0, 10000, 20000, 30000, 40000];
secondary_axis.YTickLabel = [];

print(gcf,'-r500','-dpdf','Figure17a1.pdf');

% ======================================================================== Figure 17 (a2) ======================================================================== %
f17a2 = figure;
set(gca,'FontName','Times', 'FontSize', 10);
hold on;

yyaxis left;
errorbar(scale,f17a2_100pact_tp,f17a2_100pact_sd, 'color', color_skew, 'Marker', 'o', 'MarkerFaceColor', marker_color_skew, 'MarkerEdgeColor', marker_color_skew, 'MarkerSize', 8, 'LineStyle', '-', 'LineWidth', 1.7);
errorbar(scale,f17a2_90pact_tp,f17a2_90pact_sd, 'color', color_skew, 'Marker', '^', 'MarkerFaceColor', marker_color_skew, 'MarkerEdgeColor', marker_color_skew, 'MarkerSize', 8, 'LineStyle', '--', 'LineWidth', 1.7);
errorbar(scale,f17a2_0pact_tp,f17a2_0pact_sd, 'color',color_skew, 'Marker', 's', 'MarkerFaceColor', marker_color_skew, 'MarkerEdgeColor', marker_color_skew, 'MarkerSize', 8, 'LineStyle', ':', 'LineWidth', 1.7);

set(gca, 'xcolor', 'black', 'ycolor', 'black');
ylim([0, 40000]);
yticks([0, 10000, 20000, 30000, 40000]);
yticklabels({'0', '10K', '20K', '30K', '40K'});
ylabel('Throughput [txn/sec]');

leg = legend('Skewed: 100% PACT', 'Skewed: 90% PACT', 'Skewed: 0% PACT');
set(leg, 'Location', 'northwest');
set(leg, 'FontSize', 10);
set(gcf,'Position',[400, 300, 800, 600]);

xlim([0, 35]);
xticks([4 8 16 32]);
xlabel('Number of CPUs');

yyaxis right;
ylim([0, 40000]);
yticks([0, 10000, 20000, 30000, 40000]);
yticklabels({'0', '10K', '20K', '30K', '40K'});
set(gca, 'xcolor', 'black', 'ycolor', 'black');

% set up the secondary x-axis
primary_axis = gca;
secondary_axis = axes('Position',primary_axis.Position,'XAxisLocation','top','YAxisLocation','right','Color','none', 'FontName','Times', 'FontSize', 10);
secondary_axis.XLim = [0 35];
secondary_axis.XTick = [4 8 16 32];
secondary_axis.XTickLabel = {string(f17a2_nt_tp(1)) + 'K', string(f17a2_nt_tp(2)) + 'K', string(f17a2_nt_tp(3)) + 'K', string(f17a2_nt_tp(4)) + 'K'};
secondary_axis.XLabel.String = 'NT throughput [txn/sec]';
% remove the y-labels
secondary_axis.YLim = [0, 40000];
secondary_axis.YTick = [0, 10000, 20000, 30000, 40000];
secondary_axis.YTickLabel = [];

print(gcf,'-r500','-dpdf','Figure17a2.pdf');

% ======================================================================== Figure 17 (b1) ======================================================================== %
f17b1 = figure;
set(gca,'FontName','Times', 'FontSize', 10);
hold on;

yyaxis left;
errorbar(scale,f17b1_100pact_tp,f17b1_100pact_sd, 'color', color_skew, 'Marker', 'o', 'MarkerFaceColor', marker_color_skew, 'MarkerEdgeColor', marker_color_skew, 'MarkerSize', 8, 'LineStyle', '-', 'LineWidth', 1.7);
errorbar(scale,f17b1_90pact_tp,f17b1_90pact_sd, 'color', color_skew, 'Marker', '^', 'MarkerFaceColor', marker_color_skew, 'MarkerEdgeColor', marker_color_skew, 'MarkerSize', 8, 'LineStyle', '--', 'LineWidth', 1.7);
errorbar(scale,f17b1_0pact_tp,f17b1_0pact_sd, 'color',color_skew, 'Marker', 's', 'MarkerFaceColor', marker_color_skew, 'MarkerEdgeColor', marker_color_skew, 'MarkerSize', 8, 'LineStyle', ':', 'LineWidth', 1.7);

set(gca, 'xcolor', 'black', 'ycolor', 'black');
ylim([0, 6000]);
yticks([0, 2000, 4000, 6000]);
yticklabels({'0', '2K', '4K', '6K'});
ylabel('Throughput [txn/sec]');

leg = legend('Low skew: 100% PACT', 'Low skew: 90% PACT', 'Low skew: 0% PACT');
set(leg, 'Location', 'northwest');
set(leg, 'FontSize', 10);
set(gcf,'Position',[400, 300, 800, 600]);

xlim([0, 35]);
xticks([4 8 16 32]);
xlabel('Number of CPUs');

yyaxis right;
ylim([0, 6000]);
yticks([0, 2000, 4000, 6000]);
yticklabels({'0', '2K', '4K', '6K'});
set(gca, 'xcolor', 'black', 'ycolor', 'black');

% set up the secondary x-axis
primary_axis = gca;
secondary_axis = axes('Position',primary_axis.Position,'XAxisLocation','top','YAxisLocation','right','Color','none', 'FontName','Times', 'FontSize', 10);
secondary_axis.XLim = [0 35];
secondary_axis.XTick = [4 8 16 32];
secondary_axis.XTickLabel = {string(f17b1_nt_tp(1)) + 'K', string(f17b1_nt_tp(2)) + 'K', string(f17b1_nt_tp(3)) + 'K', string(f17b1_nt_tp(4)) + 'K'};
secondary_axis.XLabel.String = 'NT throughput [txn/sec]';
% remove the y-labels
secondary_axis.YLim = [0, 6000];
secondary_axis.YTick = [0, 2000, 4000, 6000];
secondary_axis.YTickLabel = [];

print(gcf,'-r500','-dpdf','Figure17b1.pdf');

% ======================================================================== Figure 17 (a2) ======================================================================== %
f17b2 = figure;
set(gca,'FontName','Times', 'FontSize', 10);
hold on;

yyaxis left;
errorbar(scale,f17b2_100pact_tp,f17b2_100pact_sd, 'color', color_skew, 'Marker', 'o', 'MarkerFaceColor', marker_color_skew, 'MarkerEdgeColor', marker_color_skew, 'MarkerSize', 8, 'LineStyle', '-', 'LineWidth', 1.7);
errorbar(scale,f17b2_90pact_tp,f17b2_90pact_sd, 'color', color_skew, 'Marker', '^', 'MarkerFaceColor', marker_color_skew, 'MarkerEdgeColor', marker_color_skew, 'MarkerSize', 8, 'LineStyle', '--', 'LineWidth', 1.7);
errorbar(scale,f17b2_0pact_tp,f17b2_0pact_sd, 'color',color_skew, 'Marker', 's', 'MarkerFaceColor', marker_color_skew, 'MarkerEdgeColor', marker_color_skew, 'MarkerSize', 8, 'LineStyle', ':', 'LineWidth', 1.7);

set(gca, 'xcolor', 'black', 'ycolor', 'black');
ylim([0, 6000]);
yticks([0, 2000, 4000, 6000]);
yticklabels({'0', '2K', '4K', '6K'});
ylabel('Throughput [txn/sec]');

leg = legend('High skew: 100% PACT', 'High skew: 90% PACT', 'High skew: 0% PACT');
set(leg, 'Location', 'northwest');
set(leg, 'FontSize', 10);
set(gcf,'Position',[400, 300, 800, 600]);

xlim([0, 35]);
xticks([4 8 16 32]);
xlabel('Number of CPUs');

yyaxis right;
ylim([0, 6000]);
yticks([0, 2000, 4000, 6000]);
yticklabels({'0', '2K', '4K', '6K'});
set(gca, 'xcolor', 'black', 'ycolor', 'black');

% set up the secondary x-axis
primary_axis = gca;
secondary_axis = axes('Position',primary_axis.Position,'XAxisLocation','top','YAxisLocation','right','Color','none', 'FontName','Times', 'FontSize', 10);
secondary_axis.XLim = [0 35];
secondary_axis.XTick = [4 8 16 32];
secondary_axis.XTickLabel = {string(f17b2_nt_tp(1)) + 'K', string(f17b2_nt_tp(2)) + 'K', string(f17b2_nt_tp(3)) + 'K', string(f17b2_nt_tp(4)) + 'K'};
secondary_axis.XLabel.String = 'NT throughput [txn/sec]';
% remove the y-labels
secondary_axis.YLim = [0, 6000];
secondary_axis.YTick = [0, 2000, 4000, 6000];
secondary_axis.YTickLabel = [];

print(gcf,'-r500','-dpdf','Figure17b2.pdf');