from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='PP_CSNG666B',WORKFLOW_ID='689b3c0adfd64fd2a76d98d5f137fc4f', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG666B-0.0.prd-tz.6.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 12, 9, 21, 11, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('689b3c0adfd64fd2a76d98d5f137fc4f')

    csng666bJob_vol = []
    csng666bJob_volMnt = []
    csng666bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng666bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng666bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng666bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng666bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng666bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '18d617d42fc1438f96a2cfa5b1e0883d',
        'volumes': csng666bJob_vol,
        'volume_mounts': csng666bJob_volMnt,
        'env_from':csng666bJob_env,
        'task_id':'csng666bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.4',
        'arguments':["--job.name=csng666bJob", "fromDate=20241209", "toDate=20241209", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('689b3c0adfd64fd2a76d98d5f137fc4f')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng666bJob,
        Complete
    ]) 

    # authCheck >> csng666bJob >> Complete
    workflow








