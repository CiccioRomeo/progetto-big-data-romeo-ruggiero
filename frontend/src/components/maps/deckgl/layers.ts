import { HexagonLayer } from '@deck.gl/aggregation-layers';
import type { Coordinate } from '@/lib/types';

export function createHexagonLayer(data: Coordinate[], elevationScale: number) {
  return new HexagonLayer({
    id: 'heatmap',
    data,
    getPosition: (d: Coordinate) => d.position,
    getElevationWeight: (d: Coordinate) => d.intensity,
    extruded: true,
    radius: 30,
    coverage: 0.7,
    upperPercentile: 100,
    elevationRange: [0, 3000],
    elevationScale, // Valore dinamico
    colorRange: [
      [1, 152, 189],
      [73, 227, 206],
      [216, 254, 181],
      [254, 237, 177],
      [254, 173, 84],
      [209, 55, 78]
    ],
    pickable: true,
    material: {
      ambient: 0.64,
      diffuse: 0.6,
      shininess: 32,
      specularColor: [51, 51, 51]
    },
  });
}
