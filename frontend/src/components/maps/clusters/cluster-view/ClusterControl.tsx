import { useState } from 'react';
import styles from './cluster-control.module.css';

interface ClusterControlsProps {
  k: number;
  onVisualizza: (k: number) => void;
}

export function ClusterControls({ k, onVisualizza }: ClusterControlsProps) {
  const [tempValue, setTempValue] = useState(k);

  const handleVisualizza = () => {
    onVisualizza(tempValue); // Chiama la funzione di visualizzazione passata dal genitore
    console.log('Numero di cluster selezionato:', tempValue);
  };

  return (
    <div className={styles.controls}>
      <div className={styles.sliderGroup}>
        <span className={styles.label}>Numero di clusters:</span>
        <input
          type="range"
          min={3}
          max={10}
          value={tempValue}
          onChange={(e) => setTempValue(parseInt(e.target.value))}
          className={styles.slider}
        />
        <span className={styles.value}>{tempValue}</span>
      </div>
      <button onClick={handleVisualizza} className={styles.button}>
        Visualizza
      </button>
    </div>
  );
}
