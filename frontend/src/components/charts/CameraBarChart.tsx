import { Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
} from 'chart.js';
import { SearchedCamera } from '@/lib/types';

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip);

interface CameraDataProps{
    cameras: SearchedCamera[];
}

export function CameraBarChart({ cameras }: CameraDataProps) {
    const barThickness = 15; // Larghezza (o spessore) di ogni barra
    const chartHeight = cameras.length * barThickness + 700; // Altezza totale del grafico, con un po' di padding

    const data = {
      labels: cameras.map(couple => couple[0]),
      datasets: [{
        data: cameras.map(couple => couple[1]),
        backgroundColor: 'rgba(250, 96, 96, 0.8)', 
        borderColor: 'rgba(250, 96, 96, 1)', 
        borderWidth: 1,
        barThickness: barThickness,
      }],
    };
  
    const options = {
      indexAxis: 'y' as const,
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: false,
        },
        title: {
            display: false, 
            color: 'white',
            font: {
              size: 16,
            },
          },
        tooltip: {
          callbacks: {
            label: (context: any) => `${context.raw.toLocaleString()} foto`,
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
            autoSkip: false,
          },
        },
      },
    };
  
    return (
      <div style={{ height: `${chartHeight}px`, width: '100%' }}>
        <Bar data={data} options={options} />
      </div>
    );
  }
  