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
                , WORKFLOW_NAME='PP_CBON302B',WORKFLOW_ID='532d90daab7f42f6866b9f5c709ef724', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBON302B-0.0.prd-tz.0.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 15, 10, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('532d90daab7f42f6866b9f5c709ef724')

    cbon302bJob_vol = []
    cbon302bJob_volMnt = []
    cbon302bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbon302bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbon302bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cbon302bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cbon302bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbon302bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbon302bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbon302bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '49148b4aa8414b1dbb4c8cf96665d0dc',
        'volumes': cbon302bJob_vol,
        'volume_mounts': cbon302bJob_volMnt,
        'env_from':cbon302bJob_env,
        'task_id':'cbon302bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.45',
        'arguments':["--job.name=cbon302bJob", 
"requestDate="+str(datetime.now()),
"endTranDate=20240731"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('532d90daab7f42f6866b9f5c709ef724')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbon302bJob,
        Complete
    ]) 

    # authCheck >> cbon302bJob >> Complete
    workflow








