import { Chart as ChartJS, Title, Tooltip, LinearScale, PointElement, CategoryScale, ChartData } from 'chart.js';
import { Bubble } from 'react-chartjs-2';
import { FirstPost } from '@/lib/types';

// Registrazione dei moduli di Chart.js
ChartJS.register(Title, Tooltip, LinearScale, PointElement, CategoryScale);

// Definizione delle props del componente
interface BubbleChartProps {
  data: FirstPost[];
}

// Array delle etichette dei mesi con iniziali maiuscole
const monthLabels = [
  'Gennaio',
  'Febbraio',
  'Marzo',
  'Aprile',
  'Maggio',
  'Giugno',
  'Luglio',
  'Agosto',
  'Settembre',
  'Ottobre',
  'Novembre',
  'Dicembre',
];

export function BubbleChart({ data }: BubbleChartProps) {
  // Trasforma i dati in formato accettabile per Chart.js
  function normalize(value: number, min: number, max: number, newMin: number, newMax: number): number {
    return ((value - min) / (max - min)) * (newMax - newMin) + newMin;
  }  

  const minCount = Math.min(...data.flatMap(({ months }) => months.map(({ count }) => count)));
  const maxCount = Math.max(...data.flatMap(({ months }) => months.map(({ count }) => count)));
  const bubbleMinSize = 2; // Dimensione minima della bolla
  const bubbleMaxSize = 30; // Dimensione massima della bolla

  const yearsLabels = data.map(({ year }) => year);
  const chartData: ChartData<'bubble'> = {
    datasets: [
      {
        label: 'Conteggio attività',
        data: data.flatMap(({ year, months }) =>
          months.map(({ month, count }) => ({
            x: year, // Asse X: Anno
            y: month, // Asse Y: Mese (come stringa)
            r: normalize(count, minCount, maxCount, bubbleMinSize, bubbleMaxSize), // Dimensione bolla: Conteggio
            originalCount: count,
            label: `${monthLabels[month - 1]} ${year}` // Etichetta per il tooltip
          }))
        ),
        backgroundColor: 'rgba(54, 162, 235, 0.6)',
        borderColor: 'rgba(54, 162, 235, 1)',
        borderWidth: 1,
      },
    ],
  };

  const options = {
    responsive: true,
    plugins: {
      legend: {
        display: false, // Nasconde la legenda
      },
      tooltip: {
        callbacks: {
          label: (context: any) => {
            const { x, y, originalCount } = context.raw;
            return [
              `Anno: ${x}`,
              `Mese: ${monthLabels[y - 1]}`,
              `Conteggio: ${originalCount}`,
            ];
          },
        },
      },
    },
    scales: {
      x: {
        labels: yearsLabels,
        title: {
          display: true,
          text: 'Anno',
          font: {
            size: 14,
          },
        },
        ticks: {
          stepSize: 1, // Forza un passo di 1 tra gli anni
        },
        grid: {
          color: 'rgba(200, 200, 200, 0.2)',
          lineWidth: 0.5,
        },
      },
      y: {
        title: {
          display: true,
          text: 'Mese',
          font: {
            size: 14,
          },
        },
        ticks: {
          stepSize: 1,
          callback: (tickValue: string | number) => {
            if (typeof tickValue === 'number') {
              return monthLabels[tickValue - 1]; // Usa l'indice corretto per i mesi
            }
            return tickValue; // Fallback per tick non numerici
          },
        },
        grid: {
          color: 'rgba(200, 200, 200, 0.2)',
          lineWidth: 0.5,
        },
      },
    },
  };

  return <Bubble data={chartData} options={options} />;
}
