import { ArcLayer } from 'deck.gl';
import { Distances } from '@/lib/types'; // Import dei tipi

const COLORS = [
  [255, 0, 0, 200],
  [0, 255, 0, 200],
  [0, 0, 255, 200],
  [255, 255, 0, 200],
  [255, 0, 255, 200],
  [0, 255, 255, 200],
  [128, 0, 128, 200],
  [255, 165, 0, 200],
  [128, 128, 128, 200],
  [0, 128, 128, 200],
];

export function createConnectionLayer(data: Distances[], selectedClusters: Set<number>) {
  // Genera le connessioni tra i centroidi e i monumenti
  const connections = data.flatMap(({ centroid, distances }) => {
    if (selectedClusters.size > 0 && !selectedClusters.has(centroid.label)) {
      return []; // Ignora i cluster non selezionati
    }

    return distances.map((distanza) => ({
      sourcePosition: [centroid.longitude, centroid.latitude],
      targetPosition: [distanza.longitudeM, distanza.latitudeM],
      label: centroid.label,
      monumentName: distanza.monumento,
      distance: distanza.distanza,
      color: COLORS[centroid.label % COLORS.length],
    }));
  });

  return new ArcLayer({
    id: 'connection-layer',
    data: connections,
    pickable: true,
    getSourcePosition: (d: { sourcePosition: [number, number] }) => d.sourcePosition,
    getTargetPosition: (d: { targetPosition: [number, number] }) => d.targetPosition,
    getSourceColor: (d: { color: [number, number, number, number] }) => d.color,
    getTargetColor: (d: { color: [number, number, number, number] }) => d.color,
    getWidth: 2,
  });
}
