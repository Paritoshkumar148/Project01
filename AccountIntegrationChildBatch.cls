/**
 * Batch class for processing child accounts synchronously
 * Updated to work with the new AccountIntegrationService architecture
 */
public class AccountIntegrationChildBatch implements Database.Batchable<Id>, Database.AllowsCallouts {
    
    private Set<Id> childAccountIds;
    private Account parentAccount;
    private List<String> fieldsToSetPending;
    private Boolean useSynchronousProcessing;
    
    public AccountIntegrationChildBatch(Set<Id> childAccountIds, Account parentAccount, List<String> fieldsToSetPending) {
        this(childAccountIds, parentAccount, fieldsToSetPending, true);
    }
    
    public AccountIntegrationChildBatch(Set<Id> childAccountIds, Account parentAccount, List<String> fieldsToSetPending, Boolean useSynchronousProcessing) {
        this.childAccountIds = childAccountIds;
        this.parentAccount = parentAccount;
        this.fieldsToSetPending = fieldsToSetPending;
        this.useSynchronousProcessing = useSynchronousProcessing;
    }
    
    public Iterable<Id> start(Database.BatchableContext bc) {
        System.debug('Starting AccountIntegrationChildBatch for ' + childAccountIds.size() + ' child accounts');
        return childAccountIds;
    }
    
    public void execute(Database.BatchableContext bc, List<Id> scope) {
        System.debug('Processing batch of ' + scope.size() + ' child accounts');
        
        // Get the child accounts with all necessary fields
        Map<Id, Account> childAccountsMap = new Map<Id, Account>([
            SELECT Id, External_Id__c, DNB_Interface_Status__c, BDI_Interface_Status__c, BlackListInterfaceStatus__c,
                   Get_Info_for_Children__c
            FROM Account 
            WHERE Id IN :scope
        ]);
        
        AccountIntegrationService service = new AccountIntegrationService();
        List<Account> accountsToUpdate = new List<Account>();
        
        for(Id accountId : scope) {
            Account childAccount = childAccountsMap.get(accountId);
            if(childAccount != null) {
                try {
                    System.debug('Processing child account: ' + childAccount.Id + ' (External_Id: ' + childAccount.External_Id__c + ')');
                    
                    if(useSynchronousProcessing) {
                        // Process integrations synchronously to avoid creating new queueable jobs
                        service.callBNDSync(childAccount);
                        service.callBDISync(childAccount);
                        service.callBlackListSync(childAccount);
                        
                        // Set status fields to pending if needed
                        Account accountToUpdate = new Account(Id = childAccount.Id);
                        Boolean needsUpdate = false;
                        
                        for(String fieldName : fieldsToSetPending) {
                            if(fieldName == 'DNB_Interface_Status__c' && childAccount.DNB_Interface_Status__c != 'Pending') {
                                accountToUpdate.DNB_Interface_Status__c = 'Pending';
                                needsUpdate = true;
                            } else if(fieldName == 'BDI_Interface_Status__c' && childAccount.BDI_Interface_Status__c != 'Pending') {
                                accountToUpdate.BDI_Interface_Status__c = 'Pending';
                                needsUpdate = true;
                            } else if(fieldName == 'BlackListInterfaceStatus__c' && childAccount.BlackListInterfaceStatus__c != 'Pending') {
                                accountToUpdate.BlackListInterfaceStatus__c = 'Pending';
                                needsUpdate = true;
                            }
                        }
                        
                        if(needsUpdate) {
                            accountsToUpdate.add(accountToUpdate);
                        }
                    } else {
                        // Fallback: use the original initIntegrations method (but this should be avoided)
                        service.initIntegrations(childAccount);
                    }
                    
                } catch(Exception e) {
                    System.debug('Error processing child account ' + childAccount.Id + ': ' + e.getMessage());
                    // Continue processing other accounts even if one fails
                }
            }
        }
        
        // Update accounts if needed
        if(!accountsToUpdate.isEmpty()) {
            try {
                update accountsToUpdate;
                System.debug('Updated ' + accountsToUpdate.size() + ' child accounts with pending status');
            } catch(Exception e) {
                System.debug('Error updating child accounts: ' + e.getMessage());
            }
        }
    }
    
    public void finish(Database.BatchableContext bc) {
        System.debug('Finished processing child accounts for parent: ' + parentAccount.Id);
        
        // Get batch job info
        AsyncApexJob job = [SELECT Id, Status, NumberOfErrors, JobItemsProcessed, TotalJobItems 
                           FROM AsyncApexJob 
                           WHERE Id = :bc.getJobId()];
        
        System.debug('Batch job completed - Status: ' + job.Status + 
                    ', Processed: ' + job.JobItemsProcessed + '/' + job.TotalJobItems + 
                    ', Errors: ' + job.NumberOfErrors);
    }
}