import { Centroids } from '../../../../lib/types';
import { ScatterplotLayer, IconLayer } from '@deck.gl/layers';
import type { Cluster } from '@/lib/types';
import centroid from '@/components/assets/pin.png';
import monument from '@/components/assets/monument.png';

// Array di 10 colori predefiniti
const COLORS = [
  [255, 0, 0, 200], // Rosso
  [0, 255, 0, 200], // Verde
  [0, 0, 255, 200], // Blu
  [255, 255, 0, 200], // Giallo
  [255, 0, 255, 200], // Magenta
  [0, 255, 255, 200], // Ciano
  [128, 0, 128, 200], // Viola
  [255, 165, 0, 200], // Arancione
  [128, 128, 128, 200], // Grigio
  [0, 128, 128, 200], // Verde acqua
];

const monumentsInRome: { name: string; latitude: number; longitude: number }[] = [
  { name: "Colosseo", latitude: 41.8902, longitude: 12.4922 },
  { name: "Foro Romano", latitude: 41.8925, longitude: 12.4853 },
  { name: "Pantheon", latitude: 41.8986, longitude: 12.4769 },
  { name: "Fontana di Trevi", latitude: 41.9009, longitude: 12.4833 },
  { name: "Piazza Navona", latitude: 41.8992, longitude: 12.4731 },
  { name: "Basilica di San Pietro", latitude: 41.9022, longitude: 12.4539 },
  { name: "Musei Vaticani", latitude: 41.9065, longitude: 12.4536 },
  { name: "Castel Sant'Angelo", latitude: 41.9031, longitude: 12.4663 },
  { name: "Piazza di Spagna", latitude: 41.9057, longitude: 12.4823 },
  { name: "Galleria Borghese", latitude: 41.9142, longitude: 12.4923 },
];

export function createClusterLayer(clusters: Cluster[], centroids: Centroids[]) {
  const layers = [
    new ScatterplotLayer({
      id: 'cluster-layer',
      data: clusters,
      pickable: true,
      getPosition: (d: Cluster) => [d.longitude, d.latitude],
      getRadius: () => 60, // Raggio fisso per semplicità
      radiusUnits: 'meters',
      getFillColor: (d: Cluster) => {
        // Usa il colore corrispondente all'etichetta
        return COLORS[d.label];
      },
      radiusMinPixels: 0,
      radiusMaxPixels: Number.MAX_VALUE,
    }),
    new IconLayer({
      id: 'centroids-layer',
      data: centroids,
      pickable: true,
      getPosition: (d: Centroids) => [d.longitude, d.latitude],
      getSize: 40,
      iconAtlas: centroid, // Usa l'icona importata come atlas
      iconMapping: {
        marker: { x: 0, y: 0, width: 128, height: 128, anchorY: 128 },
      },
      getIcon: () => 'marker', // Usa la chiave 'marker' per tutte le icone
    })
  ];

  return layers;
}


export function createIconLayer() {
  type Monument = {
    name: string;
    latitude: number;
    longitude: number;
  };

  const layers = [
    new IconLayer({
      id: 'icon-layer',
      data: monumentsInRome,
      pickable: true,
      getPosition: (d: Monument) => [d.longitude, d.latitude],
      getSize: 40,
      sizeUnits: 'pixels',
      iconAtlas: monument, // Usa l'icona importata come atlas
      iconMapping: {
        marker: { x: 0, y: 0, width: 128, height: 128, anchorY: 128 },
      },
      getIcon: () => 'marker', // Usa la chiave 'marker' per tutte le icone
    }),
  ];
  return layers;
}
