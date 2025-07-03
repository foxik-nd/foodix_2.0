import { StyleSheet } from 'react-native';

export const commonStyles = StyleSheet.create({
  container: {
    flex: 1,
    padding: 20,
    alignItems: 'center',
    backgroundColor: '#FFF3C7',
  },
  title: {
    fontSize: 26,
    marginVertical: 10,
    fontWeight: 'bold',
    color: '#FF9100',
  },
  subtitle: {
    fontSize: 16,
    fontWeight: '600',
    color: '#4E2C00',
    marginTop: 12,
  },
  button: {
    backgroundColor: '#FF9100',
    padding: 12,
    borderRadius: 10,
    marginTop: 20,
  },
  buttonText: {
    color: '#FFF',
    fontSize: 16,
    fontWeight: 'bold',
  },
});
