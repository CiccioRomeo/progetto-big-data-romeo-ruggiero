import { AvgCommentsYear, AvgViewsYear } from '@/lib/types';
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
  import { Line } from 'react-chartjs-2';
  
  ChartJS.register(
    CategoryScale,
    LinearScale,
    PointElement,
    LineElement,
    Title,
    Tooltip,
    Legend
  );
  
  interface YearlyChartProps {
    viewsData: AvgViewsYear[];
    commentsData: AvgCommentsYear[];
  }
  
  export function YearlyChart({ viewsData, commentsData }: YearlyChartProps) {
    const data = {
      labels: viewsData.map(d => d.yearPosted),
      datasets: [
        {
          label: 'Visualizzazione medie',
          data: viewsData.map(d => d.average_views),
          borderColor: '#3b82f6',
          backgroundColor: 'rgba(59, 130, 246, 0.1)',
          fill: true,
          tension: 0.4,
        },
        {
          label: 'Commenti medi',
          data: commentsData.map(d => d.average_comments),
          borderColor: '#66BB6A',
          backgroundColor: 'rgba(102, 187, 106, 0.1)',
          fill: true,
          tension: 0.4,
        }
      ]
    };
  
    const options = {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: 'top' as const,
          labels: {
            color: 'white'
          }
        },
        title: {
          display: false,
          text: '',
          color: 'white'
        }
      },
      scales: {
        y: {
          grid: {
            color: 'rgba(255, 255, 255, 0.1)'
          },
          ticks: {
            color: '#a1a1aa'
          }
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
          <Line data={data} options={options} />
    );
  }
