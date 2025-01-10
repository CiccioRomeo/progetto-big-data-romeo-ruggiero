import { AvgData } from '@/lib/types';
import { GaugeComponent } from 'react-gauge-component';

interface GaugeChartProp {
  avg: AvgData;
}

export function GaugeChart({ avg }: GaugeChartProp) {
  // Calcolo del valore in giorni
  const days = Math.floor(avg.averageTimeToPostMinutes / (24 * 60));

  return (
    <GaugeComponent
      value={days} // Valore in giorni
      minValue={0} // Minimo 0
      maxValue={365} // Massimo 365
      type="radial"
      labels={{
        valueLabel: {
          style: { fontSize: 20 }, // Stile per il valore al centro
          formatTextValue: (value) => `${value} giorni` // Mostra "giorni"
        },
        tickLabels: {
          type: "outer", // Tick esterni
          ticks: [
            { value: 0 }, // Minimo
            { value: 90 }, // Primo intervallo
            { value: 180 }, // Secondo intervallo
            { value: 270 }, // Terzo intervallo
            { value: 365 } // Massimo
          ],
          defaultTickValueConfig: {
            style: { fontSize: 12 }, // Stile dei tick
            formatTextValue: (value) => `${value}g` // Mostra "g" accanto ai valori
          }
        }
      }}
      arc={{
        colorArray: ['#5BE12C', '#F5CD19', '#EA4228'], // Colori per i segmenti
        subArcs: [
          { limit: 120, color: '#5BE12C' }, // Verde fino a 120
          { limit: 240, color: '#F5CD19' }, // Giallo fino a 240
          { limit: 365, color: '#EA4228' }  // Rosso fino a 365
        ],
        padding: 0.02, // Spazio tra segmenti
        width: 0.3 // Larghezza dell'arco
      }}
      pointer={{
        elastic: true, // Movimento elastico
        animationDelay: 0 // Nessun ritardo
      }}
    />
  );
}
