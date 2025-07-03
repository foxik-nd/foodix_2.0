import { View, Text, StyleSheet, Image } from 'react-native';
import { commonStyles } from '../styles';

const betterOf = (p1, p2) => {
  const s1 = p1.nutriscore?.score ?? Number.MAX_SAFE_INTEGER;
  const s2 = p2.nutriscore?.score ?? Number.MAX_SAFE_INTEGER;

  if (s1 < s2) return { winner: p1, loser: p2, reason: 'un score Nutri-Score plus bas' };
  if (s2 < s1) return { winner: p2, loser: p1, reason: 'un score Nutri-Score plus bas' };

  
  const g1 = p1.nutriscore?.grade ?? 'Z';
  const g2 = p2.nutriscore?.grade ?? 'Z';
  if (g1 < g2) return { winner: p1, loser: p2, reason: `une meilleure lettre ( ${g1.toUpperCase()} < ${g2.toUpperCase()} )` };
  return { winner: p2, loser: p1, reason: `une meilleure lettre ( ${g2.toUpperCase()} < ${g1.toUpperCase()} )` };
};

export default function ComparaisonResultScreen({ route }) {
  const { product1, product2 } = route.params;
  const { winner, loser, reason } = betterOf(product1, product2);

  const isP1Winner = winner === product1;
  const bgColor    = isP1Winner ? '#2F62BD' : '#D32F2F'; 

  return (
    <View style={[styles.container, { backgroundColor: bgColor }]}>
      <Text style={styles.title}>🥇 Le meilleur produit</Text>

      {/* Carte gagnante */}
      <View style={styles.card}>
        <Text style={styles.name}>{winner.product_name}</Text>

        {winner.image_url && (
          <Image source={{ uri: winner.image_url }} style={styles.image} />
        )}

        <Text style={styles.score}>
          Nutri-Score : {winner.nutriscore?.grade?.toUpperCase()} ({winner.nutriscore?.score})
        </Text>
        <Text style={styles.score}>
          Éco-Score : {winner.ecoscore?.grade?.toUpperCase()} ({winner.ecoscore?.score})
        </Text>

        <View style={styles.reasonBox}>
          <Text style={styles.reasonText}>
            Ce produit est meilleur que « {loser.product_name} » grâce à {reason}.
          </Text>
        </View>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, justifyContent: 'center', alignItems: 'center', padding: 20 },
  title: {
    fontSize: 22,
    color: '#fff',
    fontWeight: 'bold',
    marginBottom: 20,
    textAlign: 'center',
  },
  card: {
    backgroundColor: 'white',
    borderRadius: 16,
    padding: 20,
    width: '100%',
    alignItems: 'center',
    elevation: 6,
  },
  name: { fontSize: 18, fontWeight: 'bold', textAlign: 'center', marginBottom: 10 },
  image: { width: 140, height: 140, borderRadius: 12, marginBottom: 10 },
  score: { fontSize: 15, marginVertical: 2, fontWeight: '500' },
  reasonBox: {
    marginTop: 16,
    backgroundColor: '#F5F5F5',
    borderRadius: 10,
    padding: 12,
  },
  reasonText: { fontSize: 14, textAlign: 'center', fontWeight: '500' },
});
