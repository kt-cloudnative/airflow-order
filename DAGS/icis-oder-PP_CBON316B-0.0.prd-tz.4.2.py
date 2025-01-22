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
                , WORKFLOW_NAME='PP_CBON316B',WORKFLOW_ID='743937a1030647769584b938bc5420ad', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBON316B-0.0.prd-tz.4.2'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 19, 21, 9, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('743937a1030647769584b938bc5420ad')

    cbon316bJob_vol = []
    cbon316bJob_volMnt = []
    cbon316bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbon316bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbon316bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbon316bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbon316bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbon316bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c0e1277f1b974d5fa8fb7e6bbadecdf5',
        'volumes': cbon316bJob_vol,
        'volume_mounts': cbon316bJob_volMnt,
        'env_from':cbon316bJob_env,
        'task_id':'cbon316bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=cbon316bJob", "endTranDate=20240528", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('743937a1030647769584b938bc5420ad')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbon316bJob,
        Complete
    ]) 

    # authCheck >> cbon316bJob >> Complete
    workflow








