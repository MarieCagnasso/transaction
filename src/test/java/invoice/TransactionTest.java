package invoice;

import invoice.DAO;
import invoice.CustomerEntity;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.hsqldb.cmdline.SqlFile;
import org.hsqldb.cmdline.SqlToolError;
import org.junit.After;
import org.junit.Test;
import org.junit.Ignore;
import static org.junit.Assert.*;
import org.junit.Before;

public class TransactionTest {
	
	private static DataSource myDataSource; // La source de données à utiliser
	private static Connection myConnection ;
	
	private DAO myDAO; // L'objet à tester
	private CustomerEntity myCustomer;

	@Before
	public  void setUp() throws IOException, SqlToolError, SQLException {
		// On crée la connection vers la base de test "in memory"
		myDataSource = getDataSource();
		myConnection = myDataSource.getConnection();
		// On crée le schema de la base de test
		executeSQLScript(myConnection, "schema.sql");
		// On y ajoute les triggers
		executeSQLScript(myConnection, "triggers.sql");
		// On y met des données
		executeSQLScript(myConnection, "testdata.sql");		

            	myDAO = new DAO(myDataSource);
                myCustomer = new CustomerEntity(0, "TestCustomer", "TestAddress");
	}
	
	private void executeSQLScript(Connection connexion, String filename)  throws IOException, SqlToolError, SQLException {
		// On initialise la base avec le contenu d'un fichier de test
		String sqlFilePath = this.getClass().getResource(filename).getFile();
		SqlFile sqlFile = new SqlFile(new File(sqlFilePath));

		sqlFile.setConnection(connexion);
		sqlFile.execute();
		sqlFile.closeReader();		
	}
	
	@After
	public void tearDown() throws SQLException {
                myConnection.close();
		myDAO = null; // Pas vraiment utile
	}
	
	@Test
	public void canCreateInvoice() throws Exception {
		// On calcule combien le client a de factures
		int id = myCustomer.getCustomerId();
		int before = myDAO.numberOfInvoicesForCustomer( id );
		// Un tableau de 3 productID
		int[] productIds = new int[]{0,1,2};
		// Un tableau de 3 quantites
		int[] quantities = new int[]{10, 20, 30};
		// On exécute la transaction
		myDAO.createInvoice(myCustomer, productIds, quantities);
		int after = myDAO.numberOfInvoicesForCustomer( myCustomer.getCustomerId() );
		// Le client a maintenant une facture de plus
		assertEquals(before + 1, after);		
	}
	
	// On vérifie que la création d'une facture met à jour le chiffre d'affaire du client (Trigger)
	@Test
	public void createInvoiceUpdatesTotal() throws Exception {
		// On calcule le chiffre d'aafaire du client
		int id = myCustomer.getCustomerId();
		float before = myDAO.totalForCustomer(id);
		System.out.printf("Before: %f %n", before);

		// Un tableau de 1 productID
		int[] productIds = new int[]{0}; // Le produit 0 vaut 10 €
		// Un tableau de 1 quantites
		int[] quantities = new int[]{2};
		// On exécute la transaction
		myDAO.createInvoice(myCustomer, productIds, quantities);

		float after = myDAO.totalForCustomer(id);
		System.out.printf("After: %f %n", after);

		// Le client a maintenant 2*10€ de plus

		assertEquals(before + 2f * 10f, after, 0.001f);		
	}
	
        @Test (expected = Exception.class)
        public void createInvoiceWithUnknownItem() throws Exception{
                // Un tableau de 1 productID
		int[] productIds = new int[]{0,10}; // Le produit 0 vaut 10 €
		// Un tableau de 1 quantites
		int[] quantities = new int[]{2,2};
                // On exécute la transaction
		myDAO.createInvoice(myCustomer, productIds, quantities);
        }
        
	@Test (expected = Exception.class)
        public void createInvoiceWithNegativeQuantitie() throws Exception{
                // Un tableau de 1 productID
		int[] productIds = new int[]{0}; // Le produit 0 vaut 10 €
		// Un tableau de 1 quantites
		int[] quantities = new int[]{-2};
                // On exécute la transaction
		myDAO.createInvoice(myCustomer, productIds, quantities);
        }
        
        @Test (expected = Exception.class)
        public void createInvoiceWithBadNumberOfItem() throws Exception{
                // Un tableau de 1 productID
		int[] productIds = new int[]{0,1}; // Le produit 0 vaut 10 €
		// Un tableau de 1 quantites
		int[] quantities = new int[]{2};
                // On exécute la transaction
		myDAO.createInvoice(myCustomer, productIds, quantities);
        }
        
         @Test 
        public void testNumberOfCustomers() throws Exception{
                assertEquals(myDAO.numberOfCustomers(), 1);
        }
        
	public static DataSource getDataSource() throws SQLException {
		org.hsqldb.jdbc.JDBCDataSource ds = new org.hsqldb.jdbc.JDBCDataSource();
		ds.setDatabase("jdbc:hsqldb:mem:testcase;shutdown=true");
		ds.setUser("sa");
		ds.setPassword("sa");
		return ds;
	}	
	
}
