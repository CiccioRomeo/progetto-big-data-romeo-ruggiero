import { DeckGL } from '@deck.gl/react';
import { Map } from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { INITIAL_VIEW_STATE_CLUSTER, MAP_STYLE } from '../../constants';
import { lightingEffect } from '../../lighting';
import { createClusterLayer } from './layers';

import styles from '../../MapView.module.css';
import type { Centroids, Cluster } from '@/lib/types';

interface ClusterMapProps {
  clusters: Cluster[];
  centroids: Centroids[];
  isLoading: boolean;
}

export function ClusterMapView({ clusters, centroids, isLoading }: ClusterMapProps) {
  console.log("Clusters: ",clusters);
  console.log("Centroids: ",centroids);
  const layers = clusters.length ? [createClusterLayer(clusters, centroids)] : [];

  return (
    <div className={styles.container}>
      {isLoading && <div>Caricamento dati...</div>}
      <DeckGL
        initialViewState={INITIAL_VIEW_STATE_CLUSTER}
        controller={{
          dragRotate: false,
          doubleClickZoom: false,
        }}
        layers={layers}
        effects={[lightingEffect]}
        getTooltip={({ object }: { object: any | null | undefined }) => {
          if (!object) {
            return null;
          }
          if ('name' in object) {
            // Tooltip per IconLayer
            return `${object.name}`;
          }
          if ('latitude' in object && 'longitude' in object) {
            // Tooltip per ScatterplotLayer
            return `Lat: ${object.latitude.toFixed(6)}\nLng: ${object.longitude.toFixed(6)}`;
          }
          return null;
        }}
      >
        <Map reuseMaps mapStyle={MAP_STYLE} attributionControl={false} />
      </DeckGL>
    </div>
  );
}
