% set up color
f15_color(9, 3) = 0;
f15_color(9, :) = [140/255, 140/255, 140/255];
f15_color(8, :) = [180/255, 180/255, 180/255];
f15_color(7, :) = [80/255, 80/255, 80/255];
f15_color(6, :) = [200/255, 200/255, 200/255];
f15_color(5, :) = [0, 0, 0];
f15_color(4, :) = [50/255, 50/255, 50/255];
f15_color(3, :) = [100/255, 100/255, 100/255];
f15_color(2, :) = [150/255, 150/255, 150/255];
f15_color(1, :) = [220/255, 220/255, 220/255];


% ======================================================================== Figure 15 ======================================================================== %
f15 = figure;
hold on;
set(gca,'FontName','Times', 'FontSize', 10);

latency(8, 9) = 0;
latency(8, :) = f15_act_0W1N;
latency(7, :) = f15_act_0W4N;
latency(6, :) = f15_act_1W3N;
latency(5, :) = f15_act_4W0N;
latency(4, :) = f15_orleans_0W1N;
latency(3, :) = f15_orleans_0W4N;
latency(2, :) = f15_orleans_1W3N;
latency(1, :) = f15_orleans_4W0N;

b = barh(latency, 'stacked');
for i = 1 : 9
  b(i).FaceColor = f15_color(i, :);
  b(i).EdgeColor = f15_color(i, :);
end

set(gca, 'xcolor', 'black', 'ycolor', 'black');
xlim([0 2]);
xticks([0 0.5 1 1.5 2]);
xticklabels({'0', '0.5', '1.0', '1.5', '2.0'});
ylim([0.5 8.5])
yticks([0.5 1 2 3 4 5 6 7 8 8.5]);
yticklabels({'', 'OrleansTxn: 4W+0N', 'OrleansTxn: 1W+3N', 'OrleansTxn: 0W+4N', 'OrleansTxn: 0W+1N', 'ACT: 4W+0N', 'ACT: 1W+3N', 'ACT: 0W+4N', 'ACT: 0W+1N', ''});
leg = legend('I1', 'I2', 'I3', 'I4', 'I5', 'I6', 'I7', 'I8', 'I9');
set(leg, 'Location', 'northeast');
set(leg, 'FontSize', 10);
set(leg, 'NumColumns', 1);
ylabel('');
xlabel('Latency [milliseconds]');
set(gcf,'Position',[400, 200, 800, 400]);
print(gcf,'-r500','-dpdf','Figure15.pdf');