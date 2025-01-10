import { Line } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
} from 'chart.js';
import { TopCameraPerYear } from '@/lib/types';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

interface CameraBarProps {
  cameras: TopCameraPerYear[];
}

export function CameraLineChart({ cameras }: CameraBarProps) {
  const data = {
    labels: cameras.map(t => t.year),
    datasets: [{
      data: cameras.map(t => t.count),
      backgroundColor: 'rgba(96, 165, 250, 0.8)', 
      borderColor: 'rgba(96, 165, 250, 1)', 
      borderWidth: 2,
      meta: cameras, 
    }],
  };

  const options = {
    responsive: true,
    plugins: {
      legend: {
        display: false,
      },
      title: {
        display: false,
        color: 'white',
        font: {
          size: 15, 
        },
      },
      tooltip: {
        callbacks: {
          label: (context: any) => {
            const dataset = context.dataset;
            const meta = dataset.meta as TopCameraPerYear[];
            const index = context.dataIndex;
            const camera = meta[index];
            return [
              `Marca: ${camera.make}`,
              `Modello: ${camera.model}`,
              `Foto: ${camera.count.toLocaleString()}`
            ];
          },
        },
      },
    },
    scales: {
      y: {
        grid: {
          color: 'rgba(255, 255, 255, 0.1)',
        },
        ticks: {
          color: 'white',
        },
      },
      x: {
        grid: {
          color: 'rgba(255, 255, 255, 0.1)',
        },
        ticks: {
          color: 'white',
        },
      },
    },
  };

  return <Line options={options} data={data} />;
}
