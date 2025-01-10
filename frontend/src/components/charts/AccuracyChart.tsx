import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    BarElement,
    Title,
    Tooltip,
  } from 'chart.js';
  import { Bar } from 'react-chartjs-2';
  import { formatNumber } from '../maps/clusters/utils';
import { Accuracy } from '@/lib/types';
  
  ChartJS.register(
    CategoryScale,
    LinearScale,
    BarElement,
    Title,
    Tooltip,
  );
  
  interface AccuracyChartProps {
    data: Accuracy[];
  }
  
  export function AccuracyChart({ data }: AccuracyChartProps) {
    const chartData = {
      labels: data.map(d => `${d.accuracy}`),
      datasets: [{
        label: 'Foto',
        data: data.map(d => d.count),
        backgroundColor: 'rgba(59, 130, 246, 0.8)',
        borderRadius: 4,
      }]
    };
  
    const options = {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: false
        },
        title: {
          display: true,
          text: 'Distribuzione dell\'accuracy',
          color: 'white' 
        },
        tooltip: {
          callbacks: {
            label: (context: any) => `Foto: ${formatNumber(context.raw)}`
          }
        }
      },
      scales: {
        y: {
          grid: {
            color: 'rgba(255, 255, 255, 0.1)'
          },
        },
        x: {
          grid: {
            display: false
          },
          ticks: {
            color: '#a1a1aa'
          }
        }
      }
    };
  
    return (
      <Bar data={chartData} options={options} />
    );
  }
