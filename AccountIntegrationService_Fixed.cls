/**
 * Created by stefanmihai on 01.08.2023.
 * Refactored to prevent "too many queueable jobs" error
 */

public without sharing class AccountIntegrationService {
    public AccountIntegrationService(){}
    
    public void initIntegrations(Account accountItem){
        if(!Test.isRunningTest()){
            Id jobId = System.enqueueJob(new ConsolidatedIntegrationQueueable(accountItem));
        }
    }

    public List<Account> getAccountData(Set<String> accIdSet){
        return [SELECT Id,DNB_Interface_Status__c,BDI_Interface_Status__c,BlackListInterfaceStatus__c,
                Get_Info_for_Children__c, External_Id__c
                FROM Account WHERE Id =: accIdSet];
    }

    // Consolidated queueable that handles all integrations in sequence
    public class ConsolidatedIntegrationQueueable implements Queueable, Database.AllowsCallouts {
        Account accountItem;
        String currentStep;
        
        // Constructor for initial call
        public ConsolidatedIntegrationQueueable(Account accountItem) {
            AccountIntegrationService srv = new AccountIntegrationService();
            this.accountItem = srv.getAccountData(new Set<String>{accountItem.Id})[0];
            this.currentStep = 'DNB'; // Start with DNB
        }
        
        // Constructor for continuing to next step
        public ConsolidatedIntegrationQueueable(Account accountItem, String nextStep) {
            AccountIntegrationService srv = new AccountIntegrationService();
            this.accountItem = srv.getAccountData(new Set<String>{accountItem.Id})[0];
            this.currentStep = nextStep;
        }
        
        public void execute(QueueableContext qc) {
            System.debug(LoggingLevel.INFO,'### Executing step: ' + currentStep + ' for account: ' + this.accountItem);
            
            try {
                switch on currentStep {
                    when 'DNB' {
                        executeDNBStep();
                    }
                    when 'BDI' {
                        executeBDIStep();
                    }
                    when 'CNET' {
                        executeCNETStep();
                    }
                    when 'CHILDREN' {
                        executeChildrenStep();
                    }
                }
            } catch (Exception e) {
                System.debug(LoggingLevel.ERROR, 'Error in integration step ' + currentStep + ': ' + e.getMessage());
                // You might want to log this error or handle it based on your requirements
            }
        }
        
        private void executeDNBStep() {
            if(this.accountItem.DNB_Interface_Status__c == 'Pending'){
                Utils_DandB.getDandBForAccountId(accountItem);
                System.debug('dnb finish');
            }
            
            // Continue to next step if not in test
            if(!Test.isRunningTest()) {
                System.enqueueJob(new ConsolidatedIntegrationQueueable(accountItem, 'BDI'));
            }
        }
        
        private void executeBDIStep() {
            if(this.accountItem.BDI_Interface_Status__c == 'Pending'){
                Utils_BDI.getBDIForAccountId(accountItem);
                System.debug('bdi finish');
            }
            
            // Continue to next step
            System.enqueueJob(new ConsolidatedIntegrationQueueable(accountItem, 'CNET'));
        }
        
        private void executeCNETStep() {
            if(this.accountItem.BlackListInterfaceStatus__c == 'Pending'){
                CnetService service = new CnetService();
                Account accToUpdate = service.searchWSResponse(accountItem.Id);
                if (accToUpdate != null) {
                    service.executeDML();
                    System.debug('cnet executed='+accToUpdate);
                }
            }
            
            // Check if we need to process children
            if(this.accountItem.Get_Info_for_Children__c == true) {
                System.enqueueJob(new ConsolidatedIntegrationQueueable(accountItem, 'CHILDREN'));
            }
        }
        
        private void executeChildrenStep() {
            System.debug('Processing children for account: ' + accountItem.Id);
            
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
            
            if (!ownerNumSet.isEmpty()) {
                Map<Id, Account> childAccounts = new Map<Id, Account>(
                    [SELECT Id, External_Id__c, BlackListInterfaceStatus__c, BDI_Interface_Status__c, DNB_Interface_Status__c 
                     FROM Account 
                     WHERE External_Id__c IN :ownerNumSet]
                );
                
                // Use batch for children processing but don't trigger recursive queueables
                List<String> fieldsToSetPending = new List<String>{'DNB_Interface_Status__c','BDI_Interface_Status__c','BlackListInterfaceStatus__c'};
                AccountIntegrationChildBatch childBatch = new AccountIntegrationChildBatch(childAccounts.keySet(), this.accountItem, fieldsToSetPending);
                Database.executeBatch(childBatch, 1);
            }
        }
    }

    // Alternative: Batch job for processing multiple accounts without queueables
    public class BulkIntegrationBatch implements Database.Batchable<Account>, Database.AllowsCallouts, Database.Stateful {
        private Set<Id> accountIds;
        private String integrationStep;
        
        public BulkIntegrationBatch(Set<Id> accountIds, String step) {
            this.accountIds = accountIds;
            this.integrationStep = step;
        }
        
        public List<Account> start(Database.BatchableContext bc) {
            return [SELECT Id, DNB_Interface_Status__c, BDI_Interface_Status__c, BlackListInterfaceStatus__c,
                    Get_Info_for_Children__c, External_Id__c
                    FROM Account WHERE Id IN :accountIds];
        }
        
        public void execute(Database.BatchableContext bc, List<Account> accounts) {
            AccountIntegrationService service = new AccountIntegrationService();
            
            for (Account acc : accounts) {
                switch on integrationStep {
                    when 'DNB' {
                        if (acc.DNB_Interface_Status__c == 'Pending') {
                            service.callBNDSync(acc);
                        }
                    }
                    when 'BDI' {
                        if (acc.BDI_Interface_Status__c == 'Pending') {
                            service.callBDISync(acc);
                        }
                    }
                    when 'BLACKLIST' {
                        if (acc.BlackListInterfaceStatus__c == 'Pending') {
                            service.callBlackListSync(acc);
                        }
                    }
                }
            }
        }
        
        public void finish(Database.BatchableContext bc) {
            System.debug('Bulk integration batch completed for step: ' + integrationStep);
        }
    }

    // Keep your sync methods for direct calls
    public void callBNDSync(Account accountItem){
        Utils_DandB.getDandBForAccountId(accountItem);
        System.debug('bnd finish');
    }

    public void callBDISync(Account accountItem){
        Utils_BDI.getBDIForAccountId(accountItem);
        System.debug('bdi finish');
    }

    public void callBlackListSync(Account accountItem){
        CnetService service = new CnetService();
        Account accToUpdate = service.searchWSResponse(accountItem.Id);
        if (accToUpdate != null) {
            service.executeDML();
            System.debug('cnet executed='+accToUpdate);
        }
    }
}