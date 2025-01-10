import { Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
} from 'chart.js';
import type { PopularOwner } from '@/lib/types'; 

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip);

interface TagsChartProps{
    owners: PopularOwner[];
}

export function UsersChart({ owners }: TagsChartProps) {
    const data = {
      labels: owners.map(o => o.username),
      datasets: [{
        data: owners.map(o => o.total_views),
        backgroundColor: 'rgba(96, 165, 250, 0.8)',
        borderColor: 'rgba(96, 165, 250, 1)',
        borderWidth: 1,
      }],
    };
  
    const options = {
      indexAxis: 'x' as const,
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: false,
        },
        tooltip: {
          callbacks: {
            label: (context: any) => `${context.raw.toLocaleString()} visualizzazioni`,
          },
        },
      },
      scales: {
        x: {
          grid: {
            color: 'rgba(255, 255, 255, 0.1)',
          },
          ticks: {
            color: '#a1a1aa',
            autoSkip: false, // Mostra tutte le etichette
          },
        },
        y: {
          grid: {
            display: false,
          },
          ticks: {
            color: 'white',
          },
        },
      },
    };
  
    return (
      <div style={{ height: '400px', width: '100%' }}>
        <Bar data={data} options={options} />
      </div>
    );
  }
  