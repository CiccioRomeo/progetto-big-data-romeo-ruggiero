import { ArcElement, Chart as ChartJS, Legend, Tooltip } from 'chart.js';
import { Doughnut } from 'react-chartjs-2';

ChartJS.register(
    ArcElement,
    Tooltip,
    Legend
);

interface RingProgressProps {
    value: number;
    label: string;
    count: number;
    color: string;
  }

export function RingProgress({ value, label, count, color }: RingProgressProps) {
    const data = {
        datasets: [
          {
            data: [value, 100 - value],
            backgroundColor: [color, 'rgba(255, 255, 255, 0.1)'],
            borderWidth: 0,
            cutout: '75%',
          },
        ],
      };
    
    const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
        legend: {
        display: false,
        },
        tooltip: {
        enabled: false,
        },
    },
    };
    
    return (
        <div style={{ position: 'relative', width: '200px', height: '200px' }}>
          <Doughnut data={data} options={options} />
          <div
            style={{
              position: 'absolute',
              top: '50%',
              left: '50%',
              transform: 'translate(-50%, -50%)',
              textAlign: 'center',
            }}
          >
            <div style={{ color: 'white', fontSize: '1.5rem', fontWeight: 'bold' }}>
              {count.toLocaleString()}
            </div>
            <div style={{ color: '#a1a1aa', fontSize: '0.875rem' }}>{label}</div>
          </div>
        </div>
      );
}
