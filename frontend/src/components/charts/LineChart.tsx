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

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);


interface LineChartProps {
  lineColor?: string;
  labels: string[];
  data: number[];
  title:string;
}

export function LineChartComponent({ lineColor = 'rgb(75, 192, 192)', labels, data, title}: LineChartProps) {
  
  const options = {
    responsive: true,
    plugins: {
      legend: {
        display: false,
      },
      title: {
        display: true,
        text: title,
        color: 'white',
        font: {
          size: 15, // Imposta la dimensione del titolo
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
        suggestedMax: Math.max(...data) * 1.1, // Ad esempio, il 10% sopra il valore massimo
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
  
  const chartData = {
    labels, 
    datasets: [
      {
        data, 
        borderColor: lineColor,
        backgroundColor: lineColor.replace('rgb', 'rgba').replace(')', ', 0.5)'),
      },
    ],
  };

  return <Line options={options} data={chartData} />;
}
