/**
 * Created by stefanmihai on 01.08.2023.
 * Updated to fix "too many queueable jobs added to the queue" error
 */

public without sharing class AccountIntegrationService {
    
    // Static counter to track queueable jobs in current transaction
    private static Integer queueableJobCount = 0;
    private static final Integer MAX_QUEUEABLE_JOBS = 45; // Stay under Salesforce limit of 50
    
    public AccountIntegrationService(){}
    
    public void initIntegrations(Account accountItem){
        // Check if we're approaching the queueable job limit
        if(queueableJobCount >= MAX_QUEUEABLE_JOBS || Test.isRunningTest()){
            // Process synchronously if we're near the limit or in test context
            processAccountIntegrationsSync(accountItem);
        } else {
            // Use queueable for async processing
            queueableJobCount++;
            Id jobId = System.enqueueJob(new AccountIntegrationQueueable(accountItem, true));
        }
    }

    public List<Account> getAccountData(Set<String> accIdSet){
        return [SELECT Id,DNB_Interface_Status__c,BDI_Interface_Status__c,BlackListInterfaceStatus__c,
                Get_Info_for_Children__c, External_Id__c
                FROM Account WHERE Id =: accIdSet];
    }

    /**
     * Synchronous processing method for when queueable limits are reached
     */
    public void processAccountIntegrationsSync(Account accountItem) {
        AccountIntegrationService srv = new AccountIntegrationService();
        Account fullAccountItem = srv.getAccountData(new Set<String>{accountItem.Id})[0];
        
        // Process all integrations synchronously
        callBNDSync(fullAccountItem);
        callBDISync(fullAccountItem);
        callBlackListSync(fullAccountItem);
        
        // Handle children if needed
        if(fullAccountItem.Get_Info_for_Children__c == true) {
            processChildAccountsSync(fullAccountItem);
        }
    }

    /**
     * Process child accounts synchronously to avoid queueable job explosion
     */
    private void processChildAccountsSync(Account parentAccount) {
        System.debug('Processing children synchronously for account: ' + parentAccount.Id);
        
        Map<Id, Owners_By_Register__c> ownersByRegistersList = new Map<Id, Owners_By_Register__c>(
            [SELECT Id, OwnerIDNum__c 
             FROM Owners_By_Register__c 
             WHERE RecordType.DeveloperName = 'D_B_Owners_By_Register' 
             AND Credit_Information__r.Account__c = :parentAccount.Id]
        );
        
        Set<String> ownerNumSet = new Set<String>();
        
        for (Owners_By_Register__c ownersByRegister : ownersByRegistersList.values()) {
            if (ownersByRegister.OwnerIDNum__c != parentAccount.External_Id__c) {
                ownerNumSet.add(ownersByRegister.OwnerIDNum__c);
            }
        }
        
        if(!ownerNumSet.isEmpty()) {
            List<Account> childAccounts = [
                SELECT Id, External_Id__c, BlackListInterfaceStatus__c, BDI_Interface_Status__c, DNB_Interface_Status__c 
                FROM Account 
                WHERE External_Id__c IN :ownerNumSet
            ];
            
            // Process child accounts synchronously instead of creating new queueable jobs
            for(Account childAccount : childAccounts) {
                try {
                    callBNDSync(childAccount);
                    callBDISync(childAccount);
                    callBlackListSync(childAccount);
                } catch(Exception e) {
                    System.debug('Error processing child account ' + childAccount.Id + ': ' + e.getMessage());
                    // Continue processing other children even if one fails
                }
            }
        }
    }

    /**
     * Consolidated queueable job that handles all integrations in sequence
     * This replaces the three separate queueable jobs to avoid chaining issues
     */
    public class AccountIntegrationQueueable implements Queueable, Database.AllowsCallouts {
        Account accountItem;
        Boolean processChildren;
        private List<String> fieldsToSetPending = new List<String>{'DNB_Interface_Status__c','BDI_Interface_Status__c','BlackListInterfaceStatus__c'};

        public AccountIntegrationQueueable(Account accountItem, Boolean processChildren) {
            AccountIntegrationService srv = new AccountIntegrationService();
            this.accountItem = srv.getAccountData(new Set<String>{accountItem.Id})[0];
            this.processChildren = processChildren;
        }

        public void execute(QueueableContext qc) {
            System.debug(LoggingLevel.INFO,'### Executing consolidated integration job for account: ' + this.accountItem.Id);
            
            try {
                // Execute DNB integration
                if(this.accountItem.DNB_Interface_Status__c == 'Pending'){
                    System.debug(LoggingLevel.INFO,'### executing dnb ' + this.accountItem);
                    Utils_DandB.getDandBForAccountId(accountItem);
                    System.debug('dnb finish');
                }

                // Execute BDI integration
                if(this.accountItem.BDI_Interface_Status__c == 'Pending'){
                    System.debug(LoggingLevel.INFO,'### executing bdi ' + this.accountItem);
                    Utils_BDI.getBDIForAccountId(accountItem);
                    System.debug('bdi finish');
                }

                // Execute BlackList/Cnet integration
                if(this.accountItem.BlackListInterfaceStatus__c == 'Pending'){
                    System.debug(LoggingLevel.INFO,'### executing cnet ' + this.accountItem);
                    CnetService service = new CnetService();
                    Account accToUpdate = service.searchWSResponse(accountItem.Id);
                    if (accToUpdate != null) {
                        service.executeDML();
                        System.debug('cnet executed='+accToUpdate);
                    }
                }

                // Handle children processing
                if(this.processChildren && this.accountItem.Get_Info_for_Children__c == true) {
                    System.debug('Processing children for account: ' + this.accountItem.Id);
                    
                    Map<Id, Owners_By_Register__c> ownersByRegistersList = new Map<Id, Owners_By_Register__c>(
                        [SELECT Id, OwnerIDNum__c 
                         FROM Owners_By_Register__c 
                         WHERE RecordType.DeveloperName = 'D_B_Owners_By_Register' 
                         AND Credit_Information__r.Account__c = :accountItem.Id]
                    );
                    
                    Set<String> ownerNumSet = new Set<String>();
                    
                    System.debug('###account ext id='+accountItem.External_Id__c);
                    for (Owners_By_Register__c ownersByRegister : ownersByRegistersList.values()) {
                        System.debug('###owner by reg id='+ownersByRegister.OwnerIDNum__c);
                        if (ownersByRegister.OwnerIDNum__c != accountItem.External_Id__c) {
                            ownerNumSet.add(ownersByRegister.OwnerIDNum__c);
                        }
                    }

                    if(!ownerNumSet.isEmpty()) {
                        Map<Id, Account> childAccounts = new Map<Id, Account>(
                            [SELECT Id, External_Id__c,BlackListInterfaceStatus__c,BDI_Interface_Status__c, DNB_Interface_Status__c 
                             FROM Account 
                             WHERE External_Id__c IN :ownerNumSet]
                        );
                        
                        // Use batch job for child processing to avoid creating multiple queueable jobs
                        AccountIntegrationChildBatch childBatch = new AccountIntegrationChildBatch(
                            childAccounts.keySet(), 
                            this.accountItem, 
                            this.fieldsToSetPending,
                            true // Use synchronous processing for children
                        );
                        Database.executeBatch(childBatch, 5); // Process 5 at a time to avoid limits
                    }
                }
                
            } catch(Exception e) {
                System.debug('Error in AccountIntegrationQueueable: ' + e.getMessage() + '\n' + e.getStackTraceString());
                throw e; // Re-throw to ensure proper error handling
            }
        }
    }

    // Keep the original synchronous methods for direct calls and child processing
    public void callBNDSync(Account accountItem){
        if(accountItem.DNB_Interface_Status__c == 'Pending'){
            Utils_DandB.getDandBForAccountId(accountItem);
            System.debug('bnd finish');
        }
    }

    public void callBDISync(Account accountItem){
        if(accountItem.BDI_Interface_Status__c == 'Pending'){
            Utils_BDI.getBDIForAccountId(accountItem);
            System.debug('bdi finish');
        }
    }

    public void callBlackListSync(Account accountItem){
        if(accountItem.BlackListInterfaceStatus__c == 'Pending'){
            CnetService service = new CnetService();
            Account accToUpdate = service.searchWSResponse(accountItem.Id);
            if (accToUpdate != null) {
                service.executeDML();
                System.debug('cnet executed='+accToUpdate);
            }
        }
    }

    /**
     * Method to reset the queueable job counter (useful for testing)
     */
    @TestVisible
    private static void resetQueueableJobCount() {
        queueableJobCount = 0;
    }

    /**
     * Method to get current queueable job count (useful for monitoring)
     */
    @TestVisible
    private static Integer getQueueableJobCount() {
        return queueableJobCount;
    }
}