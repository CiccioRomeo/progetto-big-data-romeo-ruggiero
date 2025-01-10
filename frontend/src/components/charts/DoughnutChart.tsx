import { Doughnut } from 'react-chartjs-2';
import { Chart as ChartJS, ArcElement, Tooltip, Legend } from 'chart.js';

ChartJS.register(ArcElement, Tooltip, Legend);

interface DoughnutChartProps {
  labels: string[];
  values: number[];
}

export function DoughnutChart({ labels, values }: DoughnutChartProps) {
  const data = {
    labels,
    datasets: [
      {
        data: values,
        backgroundColor: [
            'rgba(54, 162, 235, 0.6)', // Soft Blue
            'rgba(255, 99, 132, 0.6)', // Soft Red
            'rgba(255, 206, 86, 0.6)', // Soft Yellow
            'rgba(75, 192, 192, 0.6)', // Soft Teal
            'rgba(153, 102, 255, 0.6)', // Soft Purple
          ],
          hoverBackgroundColor: [
            'rgba(54, 162, 235, 0.8)',
            'rgba(255, 99, 132, 0.8)',
            'rgba(255, 206, 86, 0.8)',
            'rgba(75, 192, 192, 0.8)',
            'rgba(153, 102, 255, 0.8)',
          ],
      },
    ],
  };

  const options = {
    responsive: true,
    plugins: {
        title: {
            display: false,
            color: '#FFFFFF',
            font: {
              size: 18,
            },
          },
      tooltip: {
        callbacks: {
          label: (tooltipItem: any) => {
            const value = values[tooltipItem.dataIndex];
            const total = values.reduce((acc, val) => acc + val, 0);
            const percentage = ((value / total) * 100).toFixed(2);
            return `${value} (${percentage}%)`;
          },
        },
      },
      legend: {
        position: 'bottom' as const,
        labels: {
          color: '#FFFFFF', // White text color
          font: {
            size: 14,
          },
        },
      },
    },
  };

  return (
      <Doughnut data={data} options={options} />
  );
};
