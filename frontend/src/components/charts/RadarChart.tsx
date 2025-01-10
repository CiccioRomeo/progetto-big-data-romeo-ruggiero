import {
  Chart as ChartJS,
  RadialLinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend,
} from 'chart.js';
import { Radar } from 'react-chartjs-2';
import styles from '@/components/users-photos/top-owners/owner-card.module.css';
import photo_search_styles from "@/components/users-photos/photo-search/search-filters.module.css";
import type { CameraInfo } from '@/lib/types';

ChartJS.register(
  RadialLinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend
);

interface CameraStatsProps {
  cameraStats: CameraInfo[];
}

export function CameraStats({ cameraStats }: CameraStatsProps) {
  if (!cameraStats || cameraStats.length === 0) {
    return (
      <div className={photo_search_styles.noResults}>
        Non ci sono informazioni sulle camere!
      </div>
    );
  }

  // Estrai valori originali
  const originalUsageData = cameraStats.map((camera) => camera[2]);

  // Normalizza i dati tra 1 e 50
  const maxUsage = Math.max(...originalUsageData);
  const minUsage = Math.min(...originalUsageData);

  let normalizedUsageData = originalUsageData.map(
    (value) => 5 + ((value - minUsage) * (50 - 5)) / (maxUsage - minUsage)
  );

  // Applica trasformazione non lineare (radice quadrata) per enfatizzare differenze tra valori bassi
  normalizedUsageData = normalizedUsageData.map((value) => Math.sqrt(value) * 7);

  const labels = cameraStats.map((camera) => `${camera[1]}`);

  const data = {
    labels,
    datasets: [
      {
        label: 'Camera Usage',
        data: normalizedUsageData,
        backgroundColor: 'rgba(56, 189, 248, 0.2)',
        borderColor: 'rgba(56, 189, 248, 1)',
        borderWidth: 2,
        pointBackgroundColor: 'rgba(56, 189, 248, 1)',
        pointBorderColor: '#fff',
        pointHoverBackgroundColor: '#fff',
        pointHoverBorderColor: 'rgba(56, 189, 248, 1)',
      },
    ],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    scales: {
      r: {
        angleLines: {
          color: 'rgba(255, 255, 255, 0.1)',
        },
        grid: {
          color: 'rgba(255, 255, 255, 0.1)',
        },
        pointLabels: {
          color: 'white',
        },
        min: 1, // Forza il minimo della scala
        ticks: {
          display: false, // Nasconde completamente i tick
        },
      },
    },
    plugins: {
      legend: {
        display: false,
        position: 'bottom' as const,
        labels: {
          color: 'white',
        },
      },
      tooltip: {
        callbacks: {
          label: (context: any) => {
            const originalValue = originalUsageData[context.dataIndex];
            return `${context.label}: ${originalValue.toLocaleString()} usage`;
          },
        },
      },
    },
  };

  return (
    <div className={styles.radarChart}>
      <Radar data={data} options={options} />
    </div>
  );
}
