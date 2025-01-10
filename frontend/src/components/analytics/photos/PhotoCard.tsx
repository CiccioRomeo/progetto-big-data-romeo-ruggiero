import { PhotoSearched } from "@/lib/types";
import { Eye, Camera, Calendar } from "lucide-react";
import styles from './photo-card.module.css'

interface PhotoCardProps{
    photo: PhotoSearched
}

export function PhotoCard({ photo }: PhotoCardProps){

    return(
        <div className={styles.card}>
            <div className={styles.imageContainer}>
                <img src={photo.url} alt={photo.title} className={styles.image}/>
                <div className={styles.overlay}>
                    <div className={styles.stats}>
                        <div className={styles.stat}>
                            <Eye className={styles.icon} />
                            <span>{photo.views.toLocaleString()}</span>
                        </div>
                        <div className={styles.stat}>
                            <Camera className={styles.icon} />
                            <span>{photo.dateTaken}</span>
                        </div>
                        <div className={styles.stat}>
                            <Calendar className={styles.icon} />
                            <span>{photo.datePosted}</span>
                        </div>
                    </div>
                </div>
            </div>
            <div className={styles.content}>
                <h3 className={styles.title}>{photo.title}</h3>
                <h2 className={styles.photographer}>{photo.username}</h2>
                <div className={styles.tags}>
                    {photo.tags.slice(0, 4).map(tag => (
                        <span key={tag} className={styles.tag}>{tag}</span>
                    ))}
                    {photo.tags.length > 4 && (
                        <span className={styles.tag}>+{photo.tags.length - 4}</span>
                    )}
                </div>
            </div>
        </div>
    );
}
