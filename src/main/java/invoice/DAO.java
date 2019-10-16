package invoice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class DAO {

	private final DataSource myDataSource;

	/**
	 *
	 * @param dataSource la source de données à utiliser
	 */
	public DAO(DataSource dataSource) {
		this.myDataSource = dataSource;
	}

	/**
	 * Renvoie le chiffre d'affaire d'un client (somme du montant de ses factures)
	 *
	 * @param id la clé du client à chercher
	 * @return le chiffre d'affaire de ce client ou 0 si pas trouvé
	 * @throws SQLException
	 */
	public float totalForCustomer(int id) throws SQLException {
		String sql = "SELECT SUM(Total) AS Amount FROM Invoice WHERE CustomerID = ?";
		float result = 0;
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setInt(1, id); // On fixe le 1° paramètre de la requête
			try (ResultSet resultSet = statement.executeQuery()) {
				if (resultSet.next()) {
					result = resultSet.getFloat("Amount");
				}
			}
		}
		return result;
	}

	/**
	 * Renvoie le nom d'un client à partir de son ID
	 *
	 * @param id la clé du client à chercher
	 * @return le nom du client (LastName) ou null si pas trouvé
	 * @throws SQLException
	 */
	public String nameOfCustomer(int id) throws SQLException {
		String sql = "SELECT LastName FROM Customer WHERE ID = ?";
		String result = null;
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setInt(1, id);
			try (ResultSet resultSet = statement.executeQuery()) {
				if (resultSet.next()) {
					result = resultSet.getString("LastName");
				}
			}
		}
		return result;
	}

	/**
	 * Transaction permettant de créer une facture pour un client
	 *
	 * @param customer Le client
	 * @param productIDs tableau des numéros de produits à créer dans la facture
	 * @param quantities tableau des quantités de produits à facturer faux sinon Les deux tableaux doivent avoir la même
	 * taille
	 * @throws java.lang.Exception si la transaction a échoué
	 */
	public void createInvoice(CustomerEntity customer, int[] productIDs, int[] quantities) throws Exception{
		if (productIDs.length != quantities.length){
                    throw new Exception("La liste des produits et des quantité ne corresponde pas. ");
                }
                
                int total = 0;
                String sqlInvoice = "INSERT INTO Invoice (CustomerID) VALUES(?)";
                String sqlPrix = "SELECT Price FROM Product WHERE ID=?";
                String sqlItem = "INSERT INTO Item VALUES(?,?,?,?,?)";
                //List prix = new ArrayList();
                
                try (Connection connection = myDataSource.getConnection();
			PreparedStatement statement1 = connection.prepareStatement(sqlInvoice, Statement.RETURN_GENERATED_KEYS);
                        PreparedStatement statement2 = connection.prepareStatement(sqlPrix);
                        PreparedStatement statement3 = connection.prepareStatement(sqlItem);) 
                {
                    connection.setAutoCommit(false); // On démarre une transaction
                    
                    try{
                        statement1.setInt(1,customer.getCustomerId());

                        if (statement1.executeUpdate() != 1){
                            throw new Exception("échec création facture.");
                        }
                        
                        ResultSet keys = statement1.getGeneratedKeys();
                        keys.next();
                        int idFac = keys.getInt(1);

                        for (int i =0 ;i<productIDs.length;i++ ){

                           /* Récupération du prix */
                           statement2.setInt(1, productIDs[i]);
                           float prix;

                           try (ResultSet resultSet = statement2.executeQuery()) {
                                   if (resultSet.next()) {
                                       prix = resultSet.getFloat("Price");
                                   }
                                   else{
                                       throw new Exception("Produit inconnu.");
                                   }
                           }
                        
                           // creation item
                            statement3.setInt(1,idFac);
                            statement3.setInt(2,i);
                            statement3.setInt(3,productIDs[i]);
                            statement3.setInt(4,quantities[i]);
                            statement3.setFloat(5,prix );
                            
                            int numberUpdated = statement3.executeUpdate();
                            System.out.println(numberUpdated);
                            if (numberUpdated != 1){
                                throw new Exception("ERROR creation ligne facture");
                            }
                        }  
                        // Tout s'est bien passé, on peut valider la transaction
                        connection.commit();
                    }
                    catch(Exception e){
                        connection.rollback(); // On annule la transaction
                        throw e;       
                    } finally {
				 // On revient au mode de fonctionnement sans transaction
				connection.setAutoCommit(true);				
                    }
                }
        }
                
                
		
               

	/**
	 *
	 * @return le nombre d'enregistrements dans la table CUSTOMER
	 * @throws SQLException
	 */
	public int numberOfCustomers() throws SQLException {
		int result = 0;
                
		String sql = "SELECT COUNT(*) AS NUMBER FROM Customer";
		try (Connection connection = myDataSource.getConnection();
			Statement stmt = connection.createStatement()) {
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				result = rs.getInt("NUMBER");
			}
		}
		return result;
	}

	/**
	 *
	 * @param customerId la clé du client à recherche
	 * @return le nombre de bons de commande pour ce client (table PURCHASE_ORDER)
	 * @throws SQLException
	 */
	public int numberOfInvoicesForCustomer(int customerId) throws SQLException {
		int result = 0;

		String sql = "SELECT COUNT(*) AS NUMBER FROM Invoice WHERE CustomerID = ?";

		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setInt(1, customerId);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				result = rs.getInt("NUMBER");
			}
		}
		return result;
	}

	/**
	 * Trouver un Customer à partir de sa clé
	 *
	 * @param customedID la clé du CUSTOMER à rechercher
	 * @return l'enregistrement correspondant dans la table CUSTOMER, ou null si pas trouvé
	 * @throws SQLException
	 */
	CustomerEntity findCustomer(int customerID) throws SQLException {
		CustomerEntity result = null;

		String sql = "SELECT * FROM Customer WHERE ID = ?";
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setInt(1, customerID);

			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				String name = rs.getString("FirstName");
				String address = rs.getString("Street");
				result = new CustomerEntity(customerID, name, address);
			}
		}
		return result;
	}

	/**
	 * Liste des clients localisés dans un état des USA
	 *
	 * @param state l'état à rechercher (2 caractères)
	 * @return la liste des clients habitant dans cet état
	 * @throws SQLException
	 */
	List<CustomerEntity> customersInCity(String city) throws SQLException {
		List<CustomerEntity> result = new LinkedList<>();

		String sql = "SELECT * FROM Customer WHERE City = ?";
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setString(1, city);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					int id = rs.getInt("ID");
					String name = rs.getString("FirstName");
					String address = rs.getString("Street");
					CustomerEntity c = new CustomerEntity(id, name, address);
					result.add(c);
				}
			}
		}

		return result;
	}
}
