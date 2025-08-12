/**
 * Optimized AccountTriggerHandler method to prevent "too many queueable jobs" error
 */
public with sharing class AccountTriggerHandler_Optimized {
    
    // OPTIMIZED METHOD - Prevents "too many queueable jobs" error
    public void initIntegrationsFlagCheck(Map<Id,Account> oldAccountMap, Map<Id,Account> newAccountMap){
        Set<Account> accToGetDataList = initIntegrationServices(oldAccountMap, newAccountMap);
        if(!accToGetDataList.isEmpty()) {
            // Check if we have enough queueable jobs available for bulk processing
            Integer currentJobs = System.Limits.getQueueableJobs();
            Integer maxJobs = System.Limits.getLimitQueueableJobs();
            Integer availableJobs = maxJobs - currentJobs;
            Integer accountsToProcess = accToGetDataList.size();
            
            System.debug('Current queueable jobs: ' + currentJobs + '/' + maxJobs);
            System.debug('Available queueable jobs: ' + availableJobs + ', Accounts to process: ' + accountsToProcess);
            
            // Use a buffer of 5 jobs to be safe and allow for other processes
            if(availableJobs >= accountsToProcess && availableJobs > 5) {
                // Process asynchronously if we have enough job slots
                System.debug('Processing integrations asynchronously');
                for (Account item : accToGetDataList) {
                    AccountIntegrationService ais = new AccountIntegrationService();
                    ais.initIntegrations(item);
                }
            } else {
                // Process synchronously to avoid job limit issues
                System.debug('Processing integrations synchronously due to queueable job limits. Available: ' + availableJobs + ', Needed: ' + accountsToProcess);
                AccountIntegrationService ais = new AccountIntegrationService();
                for (Account item : accToGetDataList) {
                    ais.runIntegrationsSync(item);
                }
            }
        }
    }

    private Set<Account> initIntegrationServices(Map<Id,Account> oldAccountMap, Map<Id,Account> newAccountMap){
        Set<Account> accToGetDataList = new Set<Account>();

        for(Account newAcc : newAccountMap.values()){
            if(oldAccountMap != null){
                Account oldAcc = oldAccountMap.get(newAcc.Id);
                // Check if integration fields changed to 'Pending'
                if((newAcc.DNB_Interface_Status__c == 'Pending' && newAcc.DNB_Interface_Status__c != oldAcc.DNB_Interface_Status__c) ||
                        (newAcc.BDI_Interface_Status__c == 'Pending' && newAcc.BDI_Interface_Status__c != oldAcc.BDI_Interface_Status__c) ||
                        (newAcc.BlackListInterfaceStatus__c == 'Pending' && newAcc.BlackListInterfaceStatus__c != oldAcc.BlackListInterfaceStatus__c) ||
                        (newAcc.Get_Info_for_Children__c == true && newAcc.Get_Info_for_Children__c != oldAcc.Get_Info_for_Children__c)){
                    accToGetDataList.add(newAcc);
                }
            }else{
                // For new records, check if any integration status is 'Pending'
                if(newAcc.DNB_Interface_Status__c == 'Pending' || newAcc.BDI_Interface_Status__c == 'Pending' ||
                        newAcc.BlackListInterfaceStatus__c == 'Pending' || newAcc.Get_Info_for_Children__c == true){
                    accToGetDataList.add(newAcc);
                }
            }
        }
        return accToGetDataList;
    }
}