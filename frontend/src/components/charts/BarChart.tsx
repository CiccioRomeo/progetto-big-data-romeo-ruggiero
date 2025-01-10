import { Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
} from 'chart.js';
import type { HourCountData } from '@/lib/types'; 

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip);

interface HourCountDataProps{
    hourData: HourCountData[];
    barColor?: string;
    title: string;
}

export function BarChart({ hourData, barColor = 'rgba(96, 165, 250, 0.8)', title }: HourCountDataProps) {
    const data = {
      labels: hourData.map(couple => couple[0]),
      datasets: [{
        data: hourData.map(couple => couple[1]),
        backgroundColor: barColor, // Usa il colore passato come props
        borderColor: barColor.replace('0.8', '1'), // Calcola il colore del bordo con opacità 1
         borderWidth: 1,
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
            display: true, 
            text: title,
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
      <div style={{ height: '500px', width: '100%' }}>
        <Bar data={data} options={options} />
      </div>
    );
  }
  